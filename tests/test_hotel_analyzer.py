import pandas as pd
import pytest
from pandas.testing import assert_frame_equal
from datetime import date, timedelta

from hotel_analyzer_functions import clean_df, get_series_of_top_cities, get_coordinates_of_center, \
    get_day_city_max_temp, get_max_change, get_day_city_min_temp, get_max_dif

dict_for_test_basic_df = {
    "Name": ["Inturist", "Rodina", "Hotel 4", "Moskva", "Ladoga"],
    "Country": ["RU", "RU", "RU", "RU", "RU"],
    "City": ["Moscow", "Moscow", "Moscow", "Saint-Petersburg", "Saint-Petersburg"],
    "Latitude": ["40.910890", "40.910710", "199.4444", "49.802514", "49.802666"],
    "Longitude": [
        "-111.403390",
        "-111.403300",
        "-111.403490",
        "-7ff6.532350",
        "-76.53550",
    ],
}

dict_for_test_temperature_df = {
    "Moscow": [1, 2, 3, 4],
    "Moscow2": [5, 6, 7, 8],
    "Moscow3": [9, 12, 13, 14],
    "Moscow4": [1, 5, 3, 9],
    "Moscow5": [1, 2, 3, 4],
    "Moscow6": [5, 6, 7, 8],
    "Moscow7": [9, 12, 13, 14],
    "Moscow8": [1, 5, 3, 9],
    "Moscow9": [1, 2, 3, 4],
    "Moscow10": [5, 6, 7, 8],
    "Moscow11": [5, 6, 7, 8],
    "Moscow12": [5, 6, 7, 8],
}


@pytest.fixture(scope="module")
def create_test_basic_df():
    return pd.DataFrame(dict_for_test_basic_df)


@pytest.fixture(scope="module")
def create_test_temperature_df():
    date_index = pd.date_range(
        date.today() - timedelta(days=2), date.today() + timedelta(days=1), freq="d"
    )
    return pd.DataFrame(dict_for_test_temperature_df, index=date_index)


def test_clean_df(create_test_basic_df: pd.DataFrame):
    df = clean_df(create_test_basic_df)
    dict_for_compare_df = {
        "Name": ["Inturist", "Rodina", "Ladoga"],
        "Country": ["RU", "RU", "RU"],
        "City": ["Moscow", "Moscow", "Saint-Petersburg"],
        "Latitude": [40.91089, 40.91071, 49.802666],
        "Longitude": [-111.40339, -111.4033, -76.5355],
    }

    assert_frame_equal(
        pd.DataFrame(dict_for_compare_df).reset_index(drop=True),
        df.reset_index(drop=True),
        check_dtype=False,
    )


def test_top_cities(create_test_basic_df: pd.DataFrame):
    df = get_series_of_top_cities(create_test_basic_df).reset_index()
    dict_for_compare_df = {"Country": ["RU"], "City": ["Moscow_RU"]}
    assert_frame_equal(
        pd.DataFrame(dict_for_compare_df).reset_index(drop=True),
        df.reset_index(drop=True),
        check_dtype=False,
    )


def test_get_center_coords(create_test_basic_df: pd.DataFrame):
    df = clean_df(create_test_basic_df)
    result = get_coordinates_of_center(df)
    expected_result = [[40.9108, 49.802666], [-111.403345, -76.5355]]
    list_for_assert = []
    for i in result:
        list_for_assert.append(list(i))
    assert expected_result == list_for_assert


def test_get_day_city_max_temp(create_test_temperature_df: pd.DataFrame):
    assert get_day_city_max_temp(create_test_temperature_df) == (
        "2021-01-25",
        "Mos",
        14,
    )


def test_get_max_change(create_test_temperature_df: pd.DataFrame):
    assert get_max_change(create_test_temperature_df) == ("Mos", 8)


def test_get_day_city_min_temp(create_test_temperature_df: pd.DataFrame):
    assert get_day_city_min_temp(create_test_temperature_df) == ("2021-01-22", "Mo", 1)


def test_get_max_dif(create_test_temperature_df: pd.DataFrame):
    assert get_max_dif(create_test_temperature_df) == ("2021-01-24", "Mosc", 10)
