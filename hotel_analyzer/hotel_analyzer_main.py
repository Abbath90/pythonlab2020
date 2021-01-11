from hotel_analyzer.hotel_analyzer_functions import *


def hotel_analyzer_main(data: str, output_dir: str, size: int, threads: int) -> None:
    df = get_df_from_csv(data)
    df = clean_df(df)
    top_cities = get_series_of_top_cities(df)
    df = df[df["City"].isin(top_cities)].sort_values(["City"])
    df["address"] = df["Latitude"].astype(str) + " " + df["Longitude"].astype(str)
    list_of_min_and_max_temp_by_city = get_temperature(df)
    temperature_df = create_temperature_df(top_cities, list_of_min_and_max_temp_by_city)
    generate_plots(temperature_df, output_dir)
    temperature_info = get_temperature_info(temperature_df)
    print(temperature_info.values())
    create_splited_df_by_country_and_city(df, size, output_dir)
