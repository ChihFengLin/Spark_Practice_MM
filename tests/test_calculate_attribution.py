# coding: utf-8
import os
import pytest

from scripts import calculate_attribution
from lib import utils
from lib import schema


def test_remove_dupilcate_timestamp_within_time_range():

    input_timestamps = [
        1450501231, 1450501251, 1450501290, 1450501321, 1450501351, 1450501370]
    second_of_range = 60
    result = utils.remove_timestamp_within_time_range(
        input_timestamps, second_of_range)

    assert result == [1450501231, 1450501321]


def test_get_timestamp_less_than_target():

    input_timestamps = [1450501231, 1450501321, 1450501521, 1450501821]
    target_timestamp = 1450501555
    result = utils.get_timestamp_less_than_target(
        input_timestamps, target_timestamp)

    assert result == [1450501231, 1450501321, 1450501521]


@pytest.mark.usefixtures("sql_context")
def test_dedupe_click_event(sql_context):

    input_click_data = [
        (1450501449, "253ea62f-0fd2", 1, "6507f07f-be06", "click"),
        (1450501479, "253ea62f-0fd2", 1, "6507f07f-be06", "click"),
        (1450501499, "931defd3-543d", 0, "7fe40811-7d3d", "click"),
        (1450503185, "fc6a428a-7119", 1, "0adb6ada-6978", "click"),
        (1450503502, "31726fa4-be9e", 0, "9e577b56-47a2", "click")
    ]

    click_events_df = sql_context.createDataFrame(
        input_click_data, schema.EVENT_COLUMN_NAME
    )

    deduped_click_df = \
        calculate_attribution.dedupe_click_event(click_events_df, sql_context)
    effective_event_number = deduped_click_df.count()
    effective_event_result = (
        deduped_click_df
        .orderBy('timestamp', 'event_id')
        .rdd.map(tuple)
        .collect()
    )

    assert effective_event_number == 4
    assert effective_event_result == [
        (1450501449, "253ea62f-0fd2", 1, "6507f07f-be06", "click"),
        (1450501499, "931defd3-543d", 0, "7fe40811-7d3d", "click"),
        (1450503185, "fc6a428a-7119", 1, "0adb6ada-6978", "click"),
        (1450503502, "31726fa4-be9e", 0, "9e577b56-47a2", "click")
    ]


@pytest.mark.usefixtures("sql_context")
def test_group_by_user_advertiser_id(sql_context):

    input_impression_data = [
        (1450501208, 0, 1, "6507f07f-be06-4355-9067-ab7dcba6a895"),
        (1450501288, 1, 0, "a4bc67e6-9c4a-4e55-9d76-c3a19bcc225e"),
        (1450501296, 1, 4, "7fe40811-7d3b-42b9-83ff-58eee06859d9"),
        (1450501335, 1, 3, "426da729-1c9a-4705-ba95-81609304ab50"),
        (1450501353, 1, 2, "a4bc67e6-9c4a-4e55-9d76-c3a19bcc225e"),
        (1450501466, 1, 3, "9e577b56-47a2-4d6b-9754-baadb0472b79")
    ]

    impressions_df = sql_context.createDataFrame(
        input_impression_data, schema.IMPRESSION_COLUMN_NAME
    ).withColumnRenamed(
        'timestamp', 'impression_timestamp'
    )

    grouped_impression_df = \
        calculate_attribution.group_by_user_advertiser_id(impressions_df)

    result = grouped_impression_df.where(
        grouped_impression_df["user_id"] == "a4bc67e6-9c4a-4e55-9d76-c3a19bcc225e"
    ).select("impression_timestamp_list").rdd.map(list).collect()

    assert result[0][0] == [1450501288, 1450501353]


def get_joined_impression_and_event_dataframe(sql_context):

    joined_impression_and_event_data_schema = [
        "impression_timestamp_list",
        "event_timestamp",
        "event_id",
        "advertiser_id",
        "user_id",
        "event_type"
    ]

    input_joined_impression_and_event_data = [
        ([1450501222, 1450501300, 1450501315, 1450501500],
         1450501449, "253ea62f-0fd2", 1, "6507f07f-be06", "click"),
        ([1450501527, 1450501899],
         1450501499, "931defd3-543d", 0, "7fe40811-7d3d", "visit"),
        ([1450503100, 1450503888],
         1450503185, "fc6a428a-7119", 1, "0adb6ada-6978", "click"),
        ([1450503300],
         1450503502, "31726fa4-be9e", 0, "9e577b56-47a2", "purchase"),
        ([1450503300],
         1450503602, "31726fa4-be9f", 0, "9e577b56-47a2", "purchase")
    ]

    joined_impression_and_event_df = sql_context.createDataFrame(
        input_joined_impression_and_event_data,
        joined_impression_and_event_data_schema
    )

    return joined_impression_and_event_df


@pytest.mark.usefixtures("sql_context")
def test_get_potential_attributed_event(sql_context):

    joined_impression_and_event_df = \
        get_joined_impression_and_event_dataframe(sql_context)

    potential_attributed_event_df = \
        calculate_attribution.get_potential_attributed_event(
            joined_impression_and_event_df, sql_context)

    potential_attributed_event_number = potential_attributed_event_df.count()
    potential_attributed_events = (
        potential_attributed_event_df
        .orderBy('event_timestamp')
        .rdd.map(tuple)
        .collect()
    )

    assert potential_attributed_event_number == 6
    assert potential_attributed_events == [
        # events match multiple impressions
        (1450501449, "253ea62f-0fd2", 1, "6507f07f-be06", "click", 1450501222),
        (1450501449, "253ea62f-0fd2", 1, "6507f07f-be06", "click", 1450501300),
        (1450501449, "253ea62f-0fd2", 1, "6507f07f-be06", "click", 1450501315),
        (1450503185, "fc6a428a-7119", 1, "0adb6ada-6978", "click", 1450503100),
        # one impression is matched by two events
        (1450503502, "31726fa4-be9e", 0, "9e577b56-47a2", "purchase", 1450503300),
        (1450503602, "31726fa4-be9f", 0, "9e577b56-47a2", "purchase", 1450503300)
    ]


@pytest.mark.usefixtures("sql_context")
def test_get_effective_attributed_events(sql_context):

    joined_impression_and_event_df = \
        get_joined_impression_and_event_dataframe(sql_context)

    potential_attributed_event_df = (
        calculate_attribution
        .get_potential_attributed_event(joined_impression_and_event_df, sql_context)
    )

    effective_attributed_event_df = (
        calculate_attribution
        .get_effective_attributed_events(potential_attributed_event_df)
    )

    effective_attributed_event_number = effective_attributed_event_df.count()
    effective_attributed_events = (
        effective_attributed_event_df
        .orderBy('event_timestamp')
        .rdd.map(tuple)
        .collect()
    )

    assert effective_attributed_event_number == 3
    assert effective_attributed_events == [
        (1, "6507f07f-be06", "click", 1450501315, 1450501449),
        (1, "0adb6ada-6978", "click", 1450503100, 1450503185),
        (0, "9e577b56-47a2", "purchase", 1450503300, 1450503502)
    ]









