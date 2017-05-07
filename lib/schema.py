from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType


SPARK_DATA_TYPE = {
    "timestamp": IntegerType(),
    "event_id": StringType(),
    "advertiser_id": IntegerType(),
    "user_id": StringType(),
    "event_type": StringType(),
    "creative_id": IntegerType(),
    "count": IntegerType()
}

EVENT_COLUMN_NAME = [
    "timestamp",
    "event_id",
    "advertiser_id",
    "user_id",
    "event_type"
]

IMPRESSION_COLUMN_NAME = [
    "timestamp",
    "advertiser_id",
    "creative_id",
    "user_id"
]


def get_spark_schema(table_name):

    schema_column_name = None
    if table_name == "event":
        schema_column_name = EVENT_COLUMN_NAME
    elif table_name == "impression":
        schema_column_name = IMPRESSION_COLUMN_NAME
    else:
        raise ValueError("There is no matched schema for table: %s", table_name)

    struct_fields = []
    for column_name in schema_column_name:
        column_type = SPARK_DATA_TYPE.get(column_name)
        struct_fields.append(StructField(column_name, column_type, True))

    return StructType(struct_fields)


