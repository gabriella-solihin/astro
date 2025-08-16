from typing import List, Optional

import pandas as pd

from inno_utils.loggers import log

from spaceprod.utils.data_transformation import is_col_null_mask
from spaceprod.utils.imports import F, SparkDataFrame


def dup_check(
    df: SparkDataFrame,
    dims: List[str],
    assert_: Optional[bool] = True,
    msg: Optional[str] = None,
) -> bool:
    """
    Checks df for dupes based on dims primary keys.

    Parameters
    ----------
    df : SparkDataFrame
        dataframe to check for dupes
    dims : List[str]
        list of columns to check as primary keys for dupes
    assert_ : Optional[bool], optional
        if True then throws an error if dataframe contains dupes, by default True
    msg : Optional[str], optional
        msg that is printed in logs when error is thrown, by default None

    Returns
    -------
    bool
        True if dataframe contains dupes False if the dataframe doesn't contain dupes
    """

    log.info(f"Checking for dups: {dims}")

    msg = f"Dups found in: {dims}" if msg is None else msg

    # cond = df.select(*dims).dropDuplicates().count() == df.count()

    # using new way to check
    df_group = df.groupBy(*dims).count()
    cond = df_group.filter(F.col("count") > 1).limit(1).count() == 0

    if assert_:
        assert cond, msg

    return cond


def dup_check_pd(df: pd.DataFrame, dims: List[str]) -> None:
    """Similar dup check as above but for Pandas"""
    df_check = df[dims].drop_duplicates()
    cond = len(df_check) == len(df)
    msg = f"Dups found in: {dims}"
    assert cond, msg


def dup_check_find_dups(df: SparkDataFrame, dims: List[str]) -> SparkDataFrame:
    """
    Checks df for dupes based on dims primary keys and also returns all dupe records

    Parameters
    ----------
    df : SparkDataFrame
        dataframe to check for dupes
    dims : List[str]
        list of columns to check as primary keys for dupes

    Returns
    -------
    SparkDataFrame
        dataframe containing just the dupe records
    """

    df_duped = df.groupBy(*dims).count().filter(F.col("count") > 1)
    df_duped_records = df.join(df_duped, dims, "inner").orderBy(*dims)

    # sometimes when the dims have nulls, the above returns 0 rows
    if df_duped_records.limit(1).count() == 0:
        return df_duped

    return df_duped_records


def dup_check_pd_find_dups(pdf: pd.DataFrame, dims: List[str]) -> None:
    """Similar dup check as above but for Pandas"""

    pdf_agg = pdf.groupby(dims).size().reset_index()
    pdf_duped = pdf_agg.rename(columns={0: "n"}).query("n>1")
    pdf_res = pdf.set_index(dims).join(pdf_duped.set_index(dims), how="inner")
    return pdf_res


def check_lookup_coverage_pd(
    df: pd.DataFrame,
    col_looked_up: str,
    dataset_id_of_external_data: str,
    threshold: float,
):
    """
    Check and assert lookup coverage for an external mapping dataset.

    This function should be called AFTER a left-join is performed as part of
    a lookup of some information ('col_looked_up') from the external
    dataset ('dataset_id_of_external_data').

    The function ensures that % of null records of the target column
    ('col_looked_up') is below the given threshold ('threshold')

    Parameters
    ----------
    df:  pd.DataFrame
        Table being checked

    col_looked_up: str
        column name containing the target information that was "looked up"
        using external data

    dataset_id_of_external_data: str
        dataset ID of the external data, only used to formulate the proper
        and information error message for the user

    threshold: float
        Proportion (between 0 and 1) of rows that should contain valid
        data in the target column ('col_looked_up')


    Returns
    ----------
    None
    """

    log.info(f"Checking coverage for dataset: '{dataset_id_of_external_data}'")

    n_total = len(df)
    n_invalid = len(df[df[col_looked_up].isnull()])

    per_invalid = n_invalid / n_total
    per_cov = 1 - per_invalid

    msg = f"Relevancy % of '{dataset_id_of_external_data}': {round(per_cov*100,1)}%"
    log.info(msg)

    msg = f"""
    External dataset '{dataset_id_of_external_data}' is incomplete!

    It is required that at most {round(threshold * 100,1)}% of records 
    in the LEFT-hand side data can remain invalid in term of '{col_looked_up}' 
    after joining the RIGHT-hand side dataset: '{dataset_id_of_external_data}'.

    But  {round(per_invalid * 100,1)}% of records in the current join 
    contain invalid data in '{col_looked_up}'.
    """

    assert per_invalid <= threshold, msg


def check_lookup_coverage(
    df: SparkDataFrame,
    col_looked_up: str,
    dataset_id_of_external_data: str,
    threshold: float,
):
    """
    Check and assert lookup coverage for an external mapping dataset.

    This function should be called AFTER a left-join is performed as part of
    a lookup of some information ('col_looked_up') from the external
    dataset ('dataset_id_of_external_data').

    The function ensures that % of null records of the target column
    ('col_looked_up') is below the given threshold ('threshold')

    Parameters
    ----------
    df:  SparkDataFrame
        Table being checked

    col_looked_up: str
        column name containing the target information that was "looked up"
        using external data

    dataset_id_of_external_data: str
        dataset ID of the external data, only used to formulate the proper
        and information error message for the user

    threshold: float
        Proportion (between 0 and 1) of rows that should contain valid
        data in the target column ('col_looked_up')


    Returns
    ----------
    None
    """

    log.info(f"Checking coverage for dataset: '{dataset_id_of_external_data}'")

    df.cache()

    n_total = df.count()
    n_invalid = df.filter(is_col_null_mask(col_looked_up)).count()

    per_invalid = n_invalid / n_total
    per_cov = 1 - per_invalid

    msg = f"Relevancy % of '{dataset_id_of_external_data}': {round(per_cov*100,1)}%"
    log.info(msg)

    msg = f"""
    External dataset '{dataset_id_of_external_data}' is incomplete!

    It is required that at most {round(threshold * 100,1)}% of records 
    in the LEFT-hand side data can remain invalid in term of '{col_looked_up}' 
    after joining the RIGHT-hand side dataset: '{dataset_id_of_external_data}'.

    But  {round(per_invalid * 100,1)}% of records in the current join 
    contain invalid data in '{col_looked_up}'.
    """

    assert per_invalid <= threshold, msg
