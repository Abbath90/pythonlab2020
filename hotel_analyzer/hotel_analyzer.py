import pandas as pd
from zipfile import ZipFile, Path
import fnmatch
import time
import numpy as np
from collections import Counter
from geopy.adapters import AioHTTPAdapter
from geopy.geocoders import Nominatim, Bing
from geopy.extra.rate_limiter import RateLimiter, AsyncRateLimiter
from datetime import date, timedelta
import matplotlib.pyplot as plt
import urllib.request as req
import json
import math
from pathlib import Path

import asyncio


def get_df_from_csv(path_to_archive):
    with ZipFile(path_to_archive) as zipfiles:
        file_list = zipfiles.namelist()
        csv_files = fnmatch.filter(file_list, "*.csv")
        data = [pd.read_csv(zipfiles.open(file_name), sep=',', encoding='utf8') for file_name in csv_files]

    return pd.concat(data).reset_index().drop(['index'], axis=1).drop(columns=['Id'])


def clean_df(df):
    df["Latitude"] = df["Latitude"].astype('str').str.extract(r'^(-?\d+\.\d+)', expand=False).astype('float')
    df["Longitude"] = df["Longitude"].astype('str').str.extract(r'^(-?\d+\.\d+)', expand=False).astype('float')
    df = df.dropna()
    df = df.loc[(df["Latitude"] > -90) & (df["Latitude"] < 90) & (df["Longitude"] < 180) & (df["Longitude"] > -180)]
    return df


def get_most_common(series):
    x = list(series)
    my_counter = Counter(x)
    return my_counter.most_common(1)[0][0]


def get_series_of_top_cities(df):
    return df.groupby(['Country']).agg(get_most_common)['City']


def get_coordinations_of_center(df):
    lat_max = df.groupby(['City'])['Latitude'].max().sort_index()
    lat_min = df.groupby(['City'])['Latitude'].min().sort_index()
    lon_max = df.groupby(['City'])['Longitude'].max().sort_index()
    lon_min = df.groupby(['City'])['Longitude'].min().sort_index()
    return (((lat_max + lat_min) / 2).values, ((lon_max + lon_min) / 2).values)


def get_location_id_by_center_coord(coords, url):
    url = f'{url}/location/search/?lattlong={str(coords[0]) + "," + str(coords[1])}'

    with req.urlopen(url) as session:
        response = session.read().decode()
        data = json.loads(response)

        return data[0]['woeid']


def get_past_days_temp(city_id, url):
    url = f'{url}/location/{city_id}/{date.today().strftime("%Y/%m/%d")}'

    with req.urlopen(url) as session:
        response = session.read().decode()
        data = json.loads(response)

    return data


def get_today_and_next_days_temp(city_id, url):
    url = f'{url}/location/{city_id}'

    with req.urlopen(url) as session:
        response = session.read().decode()
        data = json.loads(response)

    return data


def get_temperature_in_city(city_id, url):
    past_days_temp = get_past_days_temp(city_id, url)
    today_and_next_days_temp = get_today_and_next_days_temp(city_id, url)

    last_five_days_min = [round(temp['min_temp'], 2) for temp in past_days_temp[::8][1:6]]
    last_five_days_max = [round(temp['max_temp'], 2) for temp in past_days_temp[::8][1:6]]

    today_plus_next_five_days_min = [round(temp['min_temp'], 2) for temp in
                                     today_and_next_days_temp['consolidated_weather']]
    today_plus_next_five_days_max = [round(temp['max_temp'], 2) for temp in
                                     today_and_next_days_temp['consolidated_weather']]

    return (
    last_five_days_min[::-1] + today_plus_next_five_days_min, last_five_days_max[::-1] + today_plus_next_five_days_max)


def get_temperature(df):
    metaweather_url = 'https://www.metaweather.com/api'

    center_coords = get_coordinations_of_center(df)
    location_ids_for_metaweather = [get_location_id_by_center_coord(lat_long, metaweather_url) for lat_long in
                                    tuple(map(list, zip(*get_coordinations_of_center(df))))]
    temp_in_cities = [get_temperature_in_city(city_id, metaweather_url) for city_id in location_ids_for_metaweather]
    return temp_in_cities


def get_min_and_max_temp_values(list_of_min_and_max_temp_by_city):
    list_of_min__temp_by_city = []
    list_of_max_temp_by_city = []
    for pair_list in list_of_min_and_max_temp_by_city:
        for min_list in pair_list[::2]:
            list_of_min__temp_by_city.append(min_list)
        for max_list in pair_list[1::2]:
            list_of_max_temp_by_city.append(max_list)

    return (tuple(map(list, zip(*list_of_min__temp_by_city))), tuple(map(list, zip(*list_of_max_temp_by_city))))


def create_temperature_df(top_cities, list_of_min_and_max_temp_by_city):
    date_index = pd.date_range(date.today() - timedelta(days=5), date.today() + timedelta(days=5), freq='d')
    min_and_max_temp_values = get_min_and_max_temp_values(list_of_min_and_max_temp_by_city)
    temperature_df_max = pd.DataFrame(columns=top_cities, index=date_index, data=min_and_max_temp_values[1])
    temperature_df_max = temperature_df_max.add_suffix('_max')
    temperature_df_min = pd.DataFrame(columns=top_cities, index=date_index, data=min_and_max_temp_values[0])
    temperature_df_min = temperature_df_min.add_suffix('_min')

    temperature_df = pd.concat([temperature_df_max, temperature_df_min], axis=1)
    return temperature_df.reindex(sorted(temperature_df.columns), axis=1)


def generate_plots(df):
    for max_min_pair in zip(df.columns[::2], df.columns[1::2]):
        plt.figure()
        country_code = max_min_pair[0][-6:-4]
        city_name = max_min_pair[0][:-7]
        df.loc[:, max_min_pair].plot(label='Temperature changing')
        plt.xlabel('Days')
        plt.ylabel('Degrees')
        plt.legend()
        path_to_plot = Path(f'{country_code}/{city_name}')
        path_to_plot.mkdir(parents=True, exist_ok=True)
        plt.savefig(path_to_plot / 'temperature_changing.png')


def get_day_city_max_temp(df):
    max_value = df.max().max()
    date = df[df.isin([max_value]).any(axis=1)].index
    temp_df = df.loc[date].T
    city = temp_df[temp_df.isin([max_value]).any(axis=1)].index
    return (str(date[0].date()), city[0][:-4], max_value)


def get_max_change(df):
    dif_by_city = df.iloc[-1] - df.iloc[0]
    city = dif_by_city[dif_by_city == dif_by_city.max()].index[0][:-4]
    value = round(dif_by_city[dif_by_city == dif_by_city.max()][0], 2)
    return (city, value)


def get_day_city_min_temp(df):
    min_value = df.min().min()
    date = df[df.isin([min_value]).any(axis=1)].index
    temp_df = df.loc[date].T
    city = temp_df[temp_df.isin([min_value]).any(axis=1)].index
    return (str(date[0].date()), city[0][:-4], min_value)


def get_max_dif(df):
    np_dif = df[df.columns[::2]].values - df[df.columns[1::2]].values
    value_location = np.where(np_dif == np_dif.max())
    return (str(df.index[value_location[0]][0].date()), df.columns[5 * 2][:-4], round(np_dif.max(), 2))


def index_marks(nrows, slice_size):
    return range(slice_size, math.ceil(nrows / slice_size) * slice_size, slice_size)

def split_df(dfm, slice_size):
    indices = index_marks(dfm.shape[0], slice_size)
    return np.split(dfm, indices)

def create_splited_df_by_country_and_city(df, slice_size):
    for city_name, grouped_df in df.reset_index().groupby('City'):
        list_of_dfs = split_df(grouped_df, slice_size)
        for splited_number, splited_df in enumerate(list_of_dfs):
            path_to_df = Path(f'{city_name[-2:]}/{city_name[:-3]}')
            path_to_df.mkdir(parents=True, exist_ok=True)
            name_of_file = f'data_{splited_number}.csv'
            splited_df.to_csv(path_to_df / name_of_file, index=False)



if __name__ == "__main__":
    df = get_df_from_csv('./hotels.zip')
    df['City'] = df['City'] + '_' + df['Country']
    df = clean_df(df)
    top_cities = get_series_of_top_cities(df)
    df = df[df['City'].isin(top_cities)].sort_values(['City'])
    df['address'] = (df['Latitude'].astype(str) + ' ' + df['Longitude'].astype(str))
    list_of_min_and_max_temp_by_city = get_temperature(df)
    temperature_df = create_temperature_df(top_cities, list_of_min_and_max_temp_by_city)
    generate_plots(temperature_df)
    q = get_day_city_max_temp(temperature_df)
    w = get_max_change(temperature_df)
    e = get_day_city_min_temp(temperature_df)
    r = get_max_dif(temperature_df)
    print(q, w, e, r)
    slice_size = 100
    create_splited_df_by_country_and_city(df, slice_size)



