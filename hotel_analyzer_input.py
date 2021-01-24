import fnmatch
from zipfile import ZipFile

import pandas as pd


def get_df_from_csv(path_to_archive: str) -> pd.DataFrame:
    """
    Unzip archive with csv-files, make each file as pandas dataframe and concatenate them to one dataframe.
    :param path_to_archive: Path to archive with csv-files.
    :return: Concatenated filed as a pandas dataframe.
    """
    with ZipFile(path_to_archive) as zipfiles:
        file_list = zipfiles.namelist()
        csv_files = fnmatch.filter(file_list, "*.csv")
        data = [
            pd.read_csv(zipfiles.open(file_name), sep=",", encoding="utf8")
            for file_name in csv_files
        ]

    return pd.concat(data).reset_index().drop(["index"], axis=1).drop(columns=["Id"])
