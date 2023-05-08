USE ROLE TRANSFORMER;

/**
  TODO: Put the helper functions to the stage for re-use.

  Python UDF ascending_similarity returns the cosine similarity between a time-series array and its values sorted in
  ascending order. This metric penalizes decreasing trends, but does not differentiate between flat and upward trends
  and different strength of upward trends.
 */
CREATE OR REPLACE FUNCTION ascending_similarity(from_past_day INT, to_past_day INT, moving_avg_window INT, past_day_arr ARRAY, value_arr ARRAY)
returns float
language python
runtime_version = '3.8'
handler = 'ascending_similarity'
packages = ('numpy')
as
$$
import numpy
from numpy import dot
from numpy.linalg import norm

def left_moving_average(window: int, input: list):
    """
    Returns the moving average for each index in the input list. We only include items with smaller indices in the moving
    average window (left).

    For example, with a moving average window 3, the moving average for each index will be
    index 0: input[0]
    index 1: (input[0] + input[1]) / 2
    index 3: (input[0] + input[1] + input[2]) / 3
    :param window:
    :param input:
    :return:
    """
    n = len(input)
    return [sum(input[max(0, i - window + 1):i + 1]) / min(i + 1, window) for i in range(n)]

def timeseries_mapping(from_past_day: int, to_past_day: int, past_day_arr, value_arr):
    """
    This helper function returns a time-series array for more intuitive downstream operation. The returned array's
    index 0 corresponds to the beginning of the time range (from_past_day) and the last index corresponds to the end
    of the time range (to_past_day). The array fills each index (day) with the corresponding value in value_arr and 0
    otherwise.

    For example, if from_past_day=5, to_past_day=3, past_day_arr=[5, 3, 1], value_arr=[10, 20, 30]
    the return list will be [10, 0 , 20]

    :param from_past_day: Specify the start of the time range using the relative value {build day} - {data day}.
    :param to_past_day: Specify the end of the time range using the relative value {build day} - {data day}.
    :param past_day_arr: Each item in the array is a historical date, represented as {build day} - {data day}.
    :param value_arr: Each item corresponds to the metric value in the historical date in the corresponding past_day_arr.
    :return:
    """
    timeseries_window = (from_past_day - to_past_day ) + 1
    daily_values = [0] * timeseries_window
    for pi in range(len(past_day_arr)):
        di = past_day_arr[pi] - to_past_day
        if di >=0 and di < len(daily_values):
            daily_values[di] = value_arr[pi]

    daily_values.reverse()
    return daily_values

def cosine_similarity(list1, list2):
    """
    Computes the cosine similarity between the two numerical lists.
    """
    if len(list1) != len(list2):
        return None
    a1 = numpy.array(list1)
    a2 = numpy.array(list2)
    return dot(a1, a2) / (norm(a1) * norm(a2))


def ascending_similarity(from_past_day, to_past_day, moving_avg_window, past_day_arr, value_arr):
    """
    Returns the cosine similarity between a time-series array and its values sorted in ascending order. This metric
    penalizes decreasing trends, but does not differentiate between flat and upward trends and different strength of
    upward trends. The optimal value is 1 when the time series stays flat or strictly increases. The worse value is 0
    when the time-series strictly decreases.

    :param from_past_day: Specify the start of the time range using the relative value {build day} - {data day}.
    :param to_past_day: Specify the end of the time range using the relative value {build day} - {data day}.
    :param moving_avg_window: The window to compute the moving average to smooth the time-series data.
    :param past_day_arr: Each item in the array is a historical date, represented as {build day} - {data day}.
    :param value_arr: Each item corresponds to the metric value in the historical date in the corresponding past_day_arr.
    :return:
    """
    ts = timeseries_mapping(from_past_day, to_past_day, past_day_arr, value_arr)
    moving_avg_ts = left_moving_average(moving_avg_window, ts)
    moving_avg_ascending = sorted(moving_avg_ts)

    print(moving_avg_ts)
    print(moving_avg_ascending)
    return round(cosine_similarity(moving_avg_ts, moving_avg_ascending), 4)
$$;