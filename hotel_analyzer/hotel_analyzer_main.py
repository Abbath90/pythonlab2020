from hotel_analyzer_functions import (
    clean_df,
    create_temperature_df,
    get_series_of_top_cities,
    get_temperature,
    multithread_calc_of_geohash
)
from hotel_analyzer_input import get_df_from_csv
from hotel_analyzer_output import (
    create_splited_df_by_country_and_city,
    generate_plots,
    get_temperature_info,
)

def hotel_analyzer_main(data: str, output_dir: str, size: int, threads: int) -> None:
    """
    The main function of the project. Calls functions sequentially to solve assigned tasks.
    :param data: String with path to dir with input data.
    :param output_dir: String of root of directory structure.
    :param size: Number of rows in one slice.
    :param threads: Number of threads for multithreading.
    :return: None
    """
    df = get_df_from_csv(data)
    df = clean_df(df)
    df = multithread_calc_of_geohash(df, threads)
    top_cities = get_series_of_top_cities(df)
    df = df[df["City"].isin(top_cities)].sort_values(["City"])
    list_of_min_and_max_temp_by_city = get_temperature(df)
    temperature_df = create_temperature_df(top_cities, list_of_min_and_max_temp_by_city)

    generate_plots(temperature_df, output_dir)
    get_temperature_info(temperature_df, output_dir)
    create_splited_df_by_country_and_city(df, size, output_dir)
