from hotel_analyzer.hotel_analyzer_functions import *


def hotel_analyzer_main(data, output_dir, size, threads):
    df = get_df_from_csv(data)
    df = clean_df(df)
    top_cities = get_series_of_top_cities(df)
    df = df[df["City"].isin(top_cities)].sort_values(["City"])
    df["address"] = df["Latitude"].astype(str) + " " + df["Longitude"].astype(str)
    list_of_min_and_max_temp_by_city = get_temperature(df)
    temperature_df = create_temperature_df(top_cities, list_of_min_and_max_temp_by_city)
    generate_plots(temperature_df, output_dir)
    day_city_max_temp = get_day_city_max_temp(temperature_df)
    max_change = get_max_change(temperature_df)
    day_city_min_temp = get_day_city_min_temp(temperature_df)
    max_dif = get_max_dif(temperature_df)
    print(day_city_max_temp, max_change, day_city_min_temp, max_dif)
    create_splited_df_by_country_and_city(df, size, output_dir)
