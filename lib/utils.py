import os
import sys


def remove_timestamp_within_time_range(timestamp_list, second_of_range):

    result = []
    timestamp_list = sorted(timestamp_list)

    if len(timestamp_list) > 1:
        current_timesatmp = timestamp_list[0]
        group_start_index = 0
        while current_timesatmp:
            end_time = current_timesatmp + second_of_range
            current_group = [
                timestamp for timestamp in timestamp_list[group_start_index:]
                if timestamp <= end_time
            ]

            result.append(min(current_group))

            group_start_index = timestamp_list.index(current_group[-1]) + 1
            current_timesatmp = (
                timestamp_list[group_start_index]
                if group_start_index < len(timestamp_list) else None
            )
    else:
        return timestamp_list

    return result


def get_timestamp_less_than_target(timestamp_list, target):

    result = []
    target = int(target)
    for timestamp in timestamp_list:
        timestamp = int(timestamp)
        if timestamp < target:
            result.append(timestamp)

    return result

