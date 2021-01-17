import asyncio
import json
import math
import urllib.request as req
from collections import Counter
from datetime import date, timedelta
from typing import List, Tuple

import numpy as np
import pandas as pd
from geopy.adapters import AioHTTPAdapter
from geopy.extra.rate_limiter import AsyncRateLimiter, RateLimiter
from geopy.geocoders import Bing, Nominatim


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clears input dataframe from invalid(non numeric and nonexistent) values.
    :param df: Input pandas dataframe.
    :return: Pandas dataframe after cleaning.
    """
    df["Latitude"] = (
        df["Latitude"]
        .astype("str")
        .str.extract(r"^(-?\d+\.\d+)", expand=False)
        .astype("float")
    )
    df["Longitude"] = (
        df["Longitude"]
        .astype("str")
        .str.extract(r"^(-?\d+\.\d+)", expand=False)
        .astype("float")
    )
    df = df.dropna()
    df = df.loc[
        (df["Latitude"] > -90)
        & (df["Latitude"] < 90)
        & (df["Longitude"] < 180)
        & (df["Longitude"] > -180)
    ]
    return df


def get_most_common(series: pd.Series) -> pd.Series:
    """
    Agg function for grouped dataframe. Find the most common value in pandas series.
    :param series: Pandas series.
    :return: The most common value.
    """
    x = list(series)
    my_counter = Counter(x)
    return my_counter.most_common(1)[0][0]


def get_series_of_top_cities(df: pd.DataFrame) -> pd.Series:
    """
    Find the most common cities in grouped dataframe by country.
    :param df: Input pandas dataframe.
    :return: Pandas series as list of most common cities.
    """
    df["City"] = df["City"] + "_" + df["Country"]
    return df.groupby(["Country"]).agg(get_most_common)["City"]


def get_coordinates_of_center(df: pd.DataFrame) -> Tuple[np.array, np.array]:
    """
    Calculate coordinates of center for each city in dataframe.
    :param df: Input pandas dataframe.
    :return: Tuple of latitude and longitude.
    """
    lat_max = df.groupby(["City"])["Latitude"].max().sort_index()
    lat_min = df.groupby(["City"])["Latitude"].min().sort_index()
    lon_max = df.groupby(["City"])["Longitude"].max().sort_index()
    lon_min = df.groupby(["City"])["Longitude"].min().sort_index()
    return ((lat_max + lat_min) / 2).values, ((lon_max + lon_min) / 2).values


def get_location_id_by_center_coord(coords: list, url: str) -> str:
    """
    Get location id of city by latitude and longitude on www.metaweather.com.
    :param coords: List of latitude and longitude.
    :param url: Base url string of weather web-site.
    :return: String with id of the city.
    """
    url = f'{url}/location/search/?lattlong={str(coords[0]) + "," + str(coords[1])}'

    with req.urlopen(url) as session:
        response = session.read().decode()
        data = json.loads(response)

        return data[0]["woeid"]


def get_past_days_temp(city_id: str, url: str) -> dict:
    """
    Get past five days temperature for selected city.
    :param city_id: String with city id.
    :param url: Base url string of weather web-site.
    :return: json-like dict with temperature of past five days.
    """
    url = f'{url}/location/{city_id}/{date.today().strftime("%Y/%m/%d")}'

    with req.urlopen(url) as session:
        response = session.read().decode()
        data = json.loads(response)

    return data


def get_today_and_next_days_temp(city_id: str, url: str) -> dict:
    """
    Get today and next days temperature for selected city.
    :param city_id: String with city id.
    :param url: Base url string of weather web-site.
    :return: json-like dict with temperature of today and next days temperature for selected city.
    """
    url = f"{url}/location/{city_id}"

    with req.urlopen(url) as session:
        response = session.read().decode()
        data = json.loads(response)

    return data


def get_temperature_in_city(city_id: str, url: str) -> Tuple[list, list]:
    """
    Get min and max temperature for past five days, today and next five days.
    :param city_id: String with city id.
    :param url: Base url string of weather web-site.
    :return: Tuple of min and max temperature for 11 days in the city.
    """
    past_days_temp = get_past_days_temp(city_id, url)
    today_and_next_days_temp = get_today_and_next_days_temp(city_id, url)

    last_five_days_min = [
        round(temp["min_temp"], 2) for temp in past_days_temp[::8][1:6]
    ]
    last_five_days_max = [
        round(temp["max_temp"], 2) for temp in past_days_temp[::8][1:6]
    ]

    today_plus_next_five_days_min = [
        round(temp["min_temp"], 2)
        for temp in today_and_next_days_temp["consolidated_weather"]
    ]
    today_plus_next_five_days_max = [
        round(temp["max_temp"], 2)
        for temp in today_and_next_days_temp["consolidated_weather"]
    ]
    return (
        last_five_days_min[::-1] + today_plus_next_five_days_min,
        last_five_days_max[::-1] + today_plus_next_five_days_max,
    )


def get_temperature(df: pd.DataFrame) -> list:
    """
    Get list of the lists of temperatures in the cities.
    :param df: Input pandas dataframe.
    :return: List of the lists of temperatures in the cities.
    """
    metaweather_url = "https://www.metaweather.com/api"

    center_coords = get_coordinates_of_center(df)
    location_ids_for_metaweather = [
        get_location_id_by_center_coord(lat_long, metaweather_url)
        for lat_long in tuple(map(list, zip(*center_coords)))
    ]
    temp_in_cities = [
        get_temperature_in_city(city_id, metaweather_url)
        for city_id in location_ids_for_metaweather
    ]
    return temp_in_cities


def get_min_and_max_temp_values(
    list_of_min_and_max_temp_by_city: list,
) -> Tuple[np.array, np.array]:
    """
    Create numpy 2-d array with min and max temperature for each city. Return transpose array for
    further transfer to a dataframe with a temporary index.
    :param list_of_min_and_max_temp_by_city: List of the lists of temperatures in the cities.
    :return: Tuple of np arrays.
    """
    list_of_min_temp_by_city = []
    list_of_max_temp_by_city = []
    for pair_list in list_of_min_and_max_temp_by_city:
        for min_list in pair_list[::2]:
            list_of_min_temp_by_city.append(min_list)
        for max_list in pair_list[1::2]:
            list_of_max_temp_by_city.append(max_list)
    list_of_min_temp_by_city = np.array(list_of_min_temp_by_city)
    list_of_max_temp_by_city = np.array(list_of_max_temp_by_city)

    return list_of_min_temp_by_city.T, list_of_max_temp_by_city.T


def create_temperature_df(
    top_cities: pd.Series, list_of_min_and_max_temp_by_city: list
) -> pd.DataFrame:
    """
    Create pandas dataframe from list of cities and list of min and max temperature for today, past 5 days and next 5 days.
    :param top_cities: Pandas series of cities.
    :param list_of_min_and_max_temp_by_city: List of the lists of temperatures in the cities.
    :return: Pandas dataframe with values of min and max temperatures for each city with a temporary index.
    """
    date_index = pd.date_range(
        date.today() - timedelta(days=5), date.today() + timedelta(days=5), freq="d"
    )
    min_and_max_temp_values = get_min_and_max_temp_values(
        list_of_min_and_max_temp_by_city
    )
    temperature_df_max = pd.DataFrame(
        columns=top_cities, index=date_index, data=min_and_max_temp_values[1]
    )
    temperature_df_max = temperature_df_max.add_suffix("_max")
    temperature_df_min = pd.DataFrame(
        columns=top_cities, index=date_index, data=min_and_max_temp_values[0]
    )
    temperature_df_min = temperature_df_min.add_suffix("_min")

    temperature_df = pd.concat([temperature_df_max, temperature_df_min], axis=1)
    return temperature_df.reindex(sorted(temperature_df.columns), axis=1)


def get_day_city_max_temp(df: pd.DataFrame) -> Tuple[str, str, float]:
    """
    Get a day and a city with max temperature.
    :param df: Input pandas dataframe.
    :return: Tuple of day, city and value of max temperature.
    """
    max_value = df.max().max()
    date = df[df.isin([max_value]).any(axis=1)].index
    temp_df = df.loc[date].T
    city = temp_df[temp_df.isin([max_value]).any(axis=1)].index
    return str(date[0].date()), city[0][:-4], max_value


def get_max_change(df: pd.DataFrame) -> Tuple[str, float]:
    """
    Get a city with max change of temperature.
    :param df: Input pandas dataframe.
    :return: Tuple of day and value of max temperature.
    """
    dif_by_city = df.iloc[-1] - df.iloc[0]
    city = dif_by_city[dif_by_city == dif_by_city.max()].index[0][:-4]
    value = round(dif_by_city[dif_by_city == dif_by_city.max()][0], 2)
    return city, value


def get_day_city_min_temp(df: pd.DataFrame) -> Tuple[str, str, float]:
    """
    Get a day and a city with min temperature.
    :param df: Input pandas dataframe.
    :return: Tuple of day, city and value of min temperature.
    """
    min_value = df.min().min()
    date = df[df.isin([min_value]).any(axis=1)].index
    temp_df = df.loc[date].T
    city = temp_df[temp_df.isin([min_value]).any(axis=1)].index
    return str(date[0].date()), city[0][:-4], min_value


def get_max_dif(df: pd.DataFrame) -> Tuple[str, str, float]:
    """
    Get a day and a city with max difference between min and max temperature.
    :param df: Input pandas dataframe.
    :return: Tuple of day, city and value of temperature.
    """
    np_dif = df[df.columns[::2]].values - df[df.columns[1::2]].values
    value_location = np.where(np_dif == np_dif.max())
    return (
        str(df.index[value_location[0]][0].date()),
        df.columns[5 * 2][:-4],
        round(np_dif.max(), 2),
    )


def index_marks(nrows: int, slice_size: int) -> range:
    """
    Get range object for slicing.
    :param nrows: Number of rows(hotels) of a city.
    :param slice_size: Number of rows in one slice.
    :return: Range object for slicing.
    """
    return range(slice_size, math.ceil(nrows / slice_size) * slice_size, slice_size)


def split_df(df: pd.DataFrame, slice_size: int) -> List[np.array]:
    """
    Split grouped pandas dataframe into small dataframes with fixed size.
    :param df: Grouped dataframe.
    :param slice_size:  Number of rows in one slice.
    :return: List of sliced numpy arrays.
    """
    indices = index_marks(df.shape[0], slice_size)
    return np.split(df, indices)
