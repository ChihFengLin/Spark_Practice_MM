import os
import sys
import uuid
import functools

import tornado.options
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import collect_list
from pyspark.sql.functions import udf
from pyspark.sql.functions import min, max

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from lib.schema import get_spark_schema
from lib import utils


DUPLICATED_CLICK_TIME_RANGE = 60


def _expand_impression_timestamp_list(event_impression_row):
    result = []
    event_id = event_impression_row['event_id']
    advertiser_id = event_impression_row['advertiser_id']
    user_id = event_impression_row['user_id']
    event_type = event_impression_row['event_type']
    event_timestamp = event_impression_row['event_timestamp']
    effective_impression_timestamp = \
        event_impression_row['effective_impression_timestamp'].strip('[]').split(',')

    for impression_timestamp in effective_impression_timestamp:
        if impression_timestamp:
            result.append((
                event_timestamp,
                event_id,
                advertiser_id,
                user_id,
                event_type,
                int(impression_timestamp)
            ))
        else:
            result.append(None)

    return result


def _expand_timestamp_list(click_event_row):
    result = []
    event_id = click_event_row['event_id']
    advertiser_id = click_event_row['advertiser_id']
    user_id = click_event_row['user_id']
    event_type = click_event_row['event_type']
    clean_timestamp_list = \
        click_event_row['clean_timestamp_list'].strip('[]').split(',')

    for timestamp in clean_timestamp_list:
        if timestamp:
            result.append((
                int(timestamp),
                event_id,
                advertiser_id,
                user_id,
                event_type
            ))
        else:
            result.append(None)

    return result


def save_result_to_path(output_dir, data_type, result_dataframe):
    outfile_path = os.path.join(
        output_dir,
        data_type
    )

    result_dataframe.write.option("header", "true").csv(outfile_path)


def load_file_to_dataframe(file_path, schema, sql_context):
    """
    Loads csv file into a dataframe

    @param file_path: path extension based on the relative root
    @param schema: spark format schema
    @param: sql_context

    @return dataframe
    """

    return sql_context.read.format("csv").schema(schema).load(file_path)


def dedupe_click_event(click_events_dataframe, sql_context):

    get_clean_timestamp = udf(
        functools.partial(
            utils.remove_timestamp_within_time_range,
            second_of_range=DUPLICATED_CLICK_TIME_RANGE
        )
    )

    grouped_click_events_df = click_events_dataframe.groupBy(
        ['event_id', 'advertiser_id', 'user_id', 'event_type']
    ).agg(
        collect_list("timestamp").alias('timestamp_list')
    )

    grouped_click_events_df = grouped_click_events_df.withColumn(
        'clean_timestamp_list',
        get_clean_timestamp(grouped_click_events_df['timestamp_list'])
    )

    clean_click_dataframe = sql_context.createDataFrame(
        grouped_click_events_df.select([
            grouped_click_events_df['event_id'].cast('string'),
            grouped_click_events_df['advertiser_id'].cast('int'),
            grouped_click_events_df['user_id'].cast('string'),
            grouped_click_events_df['event_type'].cast('string'),
            grouped_click_events_df['clean_timestamp_list'].cast('string')
        ]).rdd.map(_expand_timestamp_list)
        .flatMap(lambda x: x)
        .filter(lambda x: x is not None),
        [
            'timestamp',
            'event_id',
            'advertiser_id',
            'user_id',
            'event_type'
        ]
    )

    return clean_click_dataframe


def group_by_user_advertiser_id(impression_dataframe):

    grouped_impression_df = impression_dataframe.groupBy(
        ['advertiser_id', 'user_id']
    ).agg(
        collect_list("impression_timestamp").alias('impression_timestamp_list')
    )

    return grouped_impression_df


def get_potential_attributed_event(joined_event_impression_df, sql_context):
    '''
    Select the events which are happended after impression. The 'potential' means
    here we do not consider the case of one impression being matched by multiple
    events.
    '''

    get_effective_impression_timestamp = \
        udf(utils.get_timestamp_less_than_target)

    events_with_effective_impression = joined_event_impression_df.withColumn(
        'effective_impression_timestamp',
        get_effective_impression_timestamp(
            joined_event_impression_df['impression_timestamp_list'],
            joined_event_impression_df['event_timestamp']
        )
    )

    attributed_events = sql_context.createDataFrame(
        events_with_effective_impression.select([
            events_with_effective_impression['event_id'].cast('string'),
            events_with_effective_impression['advertiser_id'].cast('int'),
            events_with_effective_impression['user_id'].cast('string'),
            events_with_effective_impression['event_type'].cast('string'),
            events_with_effective_impression['event_timestamp'].cast('int'),
            events_with_effective_impression['effective_impression_timestamp'].cast('string')
        ]).rdd.map(_expand_impression_timestamp_list)
        .flatMap(lambda x: x)
        .filter(lambda x: x is not None),
        [
            'event_timestamp',
            'event_id',
            'advertiser_id',
            'user_id',
            'event_type',
            'impression_timestamp'
        ]
    )

    return attributed_events


def get_effective_attributed_events(attributed_events):
    """
    Select the first event matched with the specific impression. In some cases, 
    one event might match with multiple impressions. Here we also only select
    a impression which is most clsoe to the matched event.
    """

    effective_attributed_events = attributed_events.select(
        'advertiser_id',
        'user_id',
        'event_type',
        'impression_timestamp',
        'event_timestamp'
    ).groupBy(
        'advertiser_id',
        'user_id',
        'event_type',
        'event_timestamp'
    ).agg(
        max('impression_timestamp')
    ).withColumnRenamed(
        'max(impression_timestamp)', 'impression_timestamp'
    )

    effective_attributed_events = effective_attributed_events.select(
        'event_timestamp',
        'advertiser_id',
        'user_id',
        'event_type',
        'impression_timestamp'
    ).groupBy(
        'advertiser_id',
        'user_id',
        'event_type',
        'impression_timestamp'
    ).agg(
        min('event_timestamp')
    ).withColumnRenamed(
        'min(event_timestamp)', 'event_timestamp'
    )

    return effective_attributed_events


def group_events_by_user(effective_attributed_events):

    grouped_result = (
        effective_attributed_events
        .groupBy('advertiser_id', 'event_type', 'user_id').count()
    )

    return grouped_result


def main(events_input_dir, impressions_input_dir, output_dir,
         num_partitions, sql_context, spark_ctx):

    # --- preprocess event data ---
    event_schema = get_spark_schema("event")
    events_dataframe = load_file_to_dataframe(
        events_input_dir, event_schema, sql_context
    ).repartition(num_partitions)

    click_events_dataframe = events_dataframe.filter(
        events_dataframe['event_type'] == 'click'
    )
    non_click_events_dataframe = events_dataframe.filter(
        events_dataframe['event_type'] != 'click'
    )

    clean_click_dataframe = dedupe_click_event(
        click_events_dataframe, sql_context)
    clean_events_dataframe = (
        non_click_events_dataframe
        .union(clean_click_dataframe)
        .withColumnRenamed(
            'timestamp', 'event_timestamp'
        )
    )

    clean_events_dataframe.cache()
    clean_events_dataframe.count()

    # --- preprocess impression data ---
    impression_schema = get_spark_schema("impression")
    impressions_dataframe = load_file_to_dataframe(
        impressions_input_dir, impression_schema, sql_context
    ).withColumnRenamed(
        'timestamp', 'impression_timestamp'
    ).repartition(num_partitions)

    grouped_impressions_dataframe = \
        group_by_user_advertiser_id(impressions_dataframe)

    grouped_impressions_dataframe.cache()
    grouped_impressions_dataframe.count()

    # --- extract attributed events ---
    joined_event_impression_df = clean_events_dataframe.join(
        grouped_impressions_dataframe,
        (clean_events_dataframe['advertiser_id']
            == grouped_impressions_dataframe['advertiser_id']) &
        (clean_events_dataframe['user_id']
            == grouped_impressions_dataframe['user_id']),
        'inner'
    ).drop(
        grouped_impressions_dataframe['advertiser_id']
    ).drop(
        grouped_impressions_dataframe['user_id']
    )

    attributed_events = get_potential_attributed_event(
        joined_event_impression_df, sql_context)

    effective_attributed_events = \
        get_effective_attributed_events(attributed_events)
    effective_attributed_events.cache()
    effective_attributed_events.count()

    # --- generate count of events output ---
    count_of_events = (
        effective_attributed_events
        .groupBy('advertiser_id', 'event_type')
        .count()
        .orderBy('advertiser_id', 'event_type')
        .repartition(1)
    )
    save_result_to_path(output_dir, 'count_of_events', count_of_events)

    # --- generate count of unique users ---
    grouped_event_by_users = group_events_by_user(effective_attributed_events)
    count_of_users = (
        grouped_event_by_users
        .groupBy('advertiser_id', 'event_type')
        .count()
        .orderBy('advertiser_id', 'event_type')
        .repartition(1)
    )
    save_result_to_path(output_dir, 'count_of_users', count_of_users)


if __name__ == "__main__":

    tornado.options.define("events_input_dir", type=str)
    tornado.options.define("impressions_input_dir", type=str)
    tornado.options.define("output_dir", type=str)
    tornado.options.define("num_partitions", type=int)

    tornado.options.parse_command_line()

    events_input_dir = tornado.options.options.events_input_dir
    impressions_input_dir = tornado.options.options.impressions_input_dir
    output_dir = tornado.options.options.output_dir
    num_partitions = tornado.options.options.num_partitions

    current_path = os.path.dirname(__file__)

    import zipfile

    def ziplib():
        libpath = os.path.abspath(os.path.join(current_path, "../lib"))
        zippath = '/tmp/lib-' + str(uuid.uuid1()) + '.zip'
        zf = zipfile.PyZipFile(zippath, mode='w')

        try:
            zf.debug = 3
            zf.writepy(libpath)

            return zippath

        finally:
            zf.close()

    zip_path = None

    zip_path = ziplib()
    conf = SparkConf()
    conf.set(
        "spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    spark_ctx = SparkContext(conf=conf)
    sql_context = SQLContext(spark_ctx)
    spark_ctx.addPyFile(os.path.abspath(__file__))
    spark_ctx.addPyFile(zip_path)

    logger = spark_ctx._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

    main(
        events_input_dir,
        impressions_input_dir,
        output_dir,
        num_partitions,
        sql_context,
        spark_ctx
    )

    if zip_path:
        try:
            os.remove(zip_path)
        except:
            pass

