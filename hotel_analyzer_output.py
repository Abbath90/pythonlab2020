import json
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from hotel_analyzer_functions import (
    get_day_city_max_temp,
    get_day_city_min_temp,
    get_max_change,
    get_max_dif,
    split_df,
)


def generate_plots(df: pd.DataFrame, output_dir: str) -> None:
    """
    Generate plots of changes min and max temperature for each city  from input dataframe.
    A directory structure is created for each country with cities as a subdirectories
    in the specified as output_dir directory.
    :param df: Input pandas dataframe.
    :param output_dir: String of root of directory structure.
    :return: None
    """
    for max_min_pair in zip(df.columns[::2], df.columns[1::2]):
        plt.figure()
        country_code = max_min_pair[0][-6:-4]
        city_name = max_min_pair[0][:-7]
        df.loc[:, max_min_pair].plot(label="Temperature changing")
        plt.xlabel("Days")
        plt.ylabel("Degrees")
        plt.legend()
        path_to_plot = Path(f"{output_dir}/{country_code}/{city_name}")

        path_to_plot.mkdir(parents=True, exist_ok=True)
        plt.savefig(path_to_plot / f"{country_code}_temperature_changing.png")
        print(
            f"{path_to_plot}/{country_code}_temperature_changing.png file created."
        )


def get_temperature_info(temperature_df: pd.DataFrame, output_dir) -> None:
    """
    Get general hotel information and otput it in json-file.
    :param temperature_df: Input pandas dataframe.
    :param output_dir: String of root of directory structure.
    :return: None
    """
    general_hotel_info = {
        "day_city_max_temp": get_day_city_max_temp(temperature_df),
        "max_change": get_max_change(temperature_df),
        "day_city_min_temp": get_day_city_min_temp(temperature_df),
        "max_dif": get_max_dif(temperature_df),
    }
    path_to_file = Path(output_dir)
    path_to_file.mkdir(exist_ok=True)
    with open(path_to_file / "general_hotel_info.json", "w") as f:
        json.dump(general_hotel_info, f)
    print(f"{path_to_file}/general_hotel_info.json file created.")


def create_splited_df_by_country_and_city(
    df: pd.DataFrame, slice_size: int, output_dir: str
) -> None:
    """
    Create and save grouped by city and splited by slice size dataframes.
    :param df: Input pandas dataframe.
    :param slice_size:  Number of rows in one slice.
    :param output_dir: String of root of directory structure.
    :return: None
    """
    for city_name, grouped_df in df.reset_index().groupby("City"):
        list_of_dfs = split_df(grouped_df, slice_size)
        for splited_number, splited_df in enumerate(list_of_dfs):
            path_to_df = Path(f"{output_dir}/{city_name[-2:]}/{city_name[:-3]}")
            path_to_df.mkdir(parents=True, exist_ok=True)
            name_of_file = f"{city_name}_data_{splited_number}.csv"
            splited_df.to_csv(path_to_df / name_of_file, index=False)
            print(f"{path_to_df}/{name_of_file} file created.")
