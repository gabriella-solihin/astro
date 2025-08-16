from functools import reduce
from itertools import chain
from operator import and_, or_
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd

from inno_utils.loggers import log
from pyspark.sql import Column as SparkColumn
from pyspark.sql.column import Column

from spaceprod.utils.imports import F, SparkDataFrame, T


def align_schemas_and_union(
    df_left: SparkDataFrame, df_right: SparkDataFrame
) -> SparkDataFrame:
    """Union two tables that do not have the same set of columns, missing column from each table will be filled as None
    https://stackoverflow.com/questions/39758045

    Parameters
    ----------
    df_left:
        Left table

    df_right:
        Right table

    Returns
    -------
    :
        Unioned table
    """
    left_types = {f.name: f.dataType for f in df_left.schema}
    right_types = {f.name: f.dataType for f in df_right.schema}
    left_fields = set((f.name, f.dataType) for f in df_left.schema)
    right_fields = set((f.name, f.dataType) for f in df_right.schema)

    # First go over left-unique fields
    for l_name, l_type in left_fields.difference(right_fields):
        if l_name in right_types:
            r_type = right_types[l_name]
            if l_type != r_type:
                raise TypeError(
                    f"Union failed. Type conflict on field {l_name}. "
                    f"left type {l_type}, right type {r_type}"
                )
        df_right = df_right.withColumn(l_name, F.lit(None).cast(l_type))

    # Now go over right-unique fields
    for r_name, r_type in right_fields.difference(left_fields):
        if r_name in left_types:
            l_type = left_types[r_name]
            if r_type != l_type:
                raise TypeError(
                    f"Union failed. Type conflict on field {r_name}. "
                    f"right type {r_type}, left type {l_type}"
                )
        df_left = df_left.withColumn(r_name, F.lit(None).cast(r_type))

    # Make sure columns are in the same order
    df_left = df_left.select(df_right.columns)

    return df_left.union(df_right)


def union_dfs(
    dfs: List[SparkDataFrame], *, align_schemas: bool = False
) -> SparkDataFrame:
    """Function to create Spark union from a list of data frames

    Parameters
    ----------
    dfs:
        List of Spark data frames

    align_schemas:
        Flag to indicate whether input data frames have different columns and need to be aligned

    Returns
    -------
    :
        Unioned data frame
    """
    if not align_schemas:
        dfs_unioned = reduce(SparkDataFrame.union, dfs)
    else:
        dfs_unioned = reduce(align_schemas_and_union, dfs)
    return dfs_unioned


def pyspark_df_col_to_list(df: SparkDataFrame, colname: str) -> List[Any]:
    """
    Return spark column as a list to collect to memory

    Parameters
    ----------
    df : SparkDataFrame
        any spark dataframe
    colname : str
        column name of dataframe to collect

    Returns
    -------
    List[Any]
        collected list in column within dataframe
    """

    return [x[colname] for x in df.select(colname).collect()]


def pyspark_df_col_to_unique_list(df: SparkDataFrame, colname: str) -> List[Any]:
    """
    return a list of distinct values contained in colname

    Parameters
    ----------
    df : SparkDataFrame
        dataframe that contains colname
    colname : str
        name of the column that we want to get distinct values from

    Returns
    -------
    List[Any]
        [description]
    """
    assert colname in df.columns, f"{colname} does not exist in df"
    return pyspark_df_col_to_list(df.select(colname).dropDuplicates(), colname)


def col_array_maps(list_dicts: List[Dict[Any, Any]]):
    """
    Generates a STATIC column containing an array of maps using the data
    from the given list of dictionaries

    Parameters
    ----------
    list_dicts: a list of dictionaries

    Returns
    -------
    pyspark column
    """

    list_ = [F.create_map([F.lit(x) for x in chain(*x.items())]) for x in list_dicts]
    col = F.array(list_)
    return col


def is_col_null_mask(col_name: str):
    """
    This function returns a universal mask that can be used to check i  f a
    column has an "invalid" value regardless of its type.
    It uses a list of conditions separated by "OR" operator.

    Parameters
    ----------
    col_name: column name to check

    Returns
    -------
    a mask for filtering a spark df
    """
    x = col_name

    # if any of these are satisfied, the mask will yield 'True'
    list_of_conditions = [
        F.col(x).isNull(),
        F.isnull(F.col(x)),
        F.isnan(F.col(x)),
        F.trim(F.lower(F.col(x))) == "null",
        F.trim(F.lower(F.col(x))) == "nan",
        F.trim(F.lower(F.col(x))) == "none",
        F.trim(F.lower(F.col(x))) == "na",
        F.trim(F.lower(F.col(x))) == "",
        F.trim(F.lower(F.col(x))) == " ",
    ]

    mask = reduce(or_, list_of_conditions)

    return mask


def filter_df_to_invalid(
    df: SparkDataFrame, cols_with_null: List[str], is_any: Optional[bool] = True
) -> SparkDataFrame:
    """
    Checks if any (if is_any == True) or if all (if is_any == False)
    columns are invalid and return only those rows

    Parameters
    ----------
    df:
        dataframe to filter

    cols_with_null:
        list of columns to filter on for invalid records

    is_any:
        if True, will filter to where ANY of the given columns are invalid,
        otherwise where ALL of them are invalid

    Returns
    -------
    filtered data
    """

    oper = or_ if is_any else and_

    mask = reduce(oper, [is_col_null_mask(x) for x in cols_with_null])

    df_result = df.filter(mask)

    return df_result


def check_invalid(
    df: SparkDataFrame,
    cols_with_null: List[str],
    is_any: Optional[bool] = True,
) -> None:

    """
    Asserts that none of (if is_any == True) or all of (if is_any == False)
    columns are invalid and return only those rows

    Parameters
    ----------
    df:
        dataframe to check

    cols_with_null:
        list of columns to filter on for invalid records

    is_any:
        if True, will filter to where ANY of the given columns are invalid,
        otherwise where ALL of them are invalid

    Returns
    -------
    None
    """

    log.info(f"Checking for invalid records in cols: {cols_with_null}")

    df_check = filter_df_to_invalid(
        df=df,
        cols_with_null=cols_with_null,
        is_any=is_any,
    )

    msg_oper = "SOME" if is_any else "ALL"
    msg = f"Invalid records found in {msg_oper} of these columns: {cols_with_null}"
    assert df_check.limit(1).count() == 0, msg


def df_to_dict(df: SparkDataFrame) -> List[Dict[str, Any]]:
    """
    converts a spark DF to a local python list of dictionaries on
    the driver node
    """

    coll_df = df.collect()
    result = [x.asDict() for x in coll_df]
    return result


def get_single_value(pdf: pd.DataFrame, col: str) -> Any:
    """
     returns a single value from a columns from a Pandas DF
     the column must have a single value

    Parameters
    ----------
    pdf: a Pandas dataframe
    col: column name containing the value

    Returns
    -------
    value
    """
    list_vals = pdf[col].unique()
    assert len(list_vals) == 1
    val = list_vals[0]
    return val


def apply_data_definition_pd(
    pdf: pd.DataFrame,
    data_contract: List[Tuple[str, str, type]],
    drop_if_not_in_data_contract: Optional[bool] = False,
):
    """
    Attempts to convert column names and types on a PDF to comply with the
    given data contract. Breaks if column is not found or type cannot
    be converted.

    Parameters
    ----------
    pdf : pd.DataFrame
        dataset to convert

    data_contract : List[Tuple[str, str, type]]
        data contract represented as a list of tuples of the following
        structure:
        (<original column name>, <new column name>, <data type>)

    drop_if_not_in_data_contract: bool
        if true, columns that are not in the 'data_contract' will be dropped

    Returns
    -------
    converted data
    """

    for col_original, col_new, data_type in data_contract:
        ren = {col_original: col_new}
        pdf = pdf.rename(ren, axis=1, errors="raise", inplace=False)
        pdf[col_new] = pdf[col_new].astype(data_type)

    if drop_if_not_in_data_contract:
        cols = [x[1] for x in data_contract]
        pdf = pdf[cols]

    return pdf


def apply_data_definition(
    df: SparkDataFrame,
    data_contract: List[Tuple[str, str, T.DataType]],
    drop_if_not_in_data_contract: Optional[bool] = False,
):
    """
    Attempts to convert column names and types on a DF to comply with the
    given data contract. Breaks if column is not found or type cannot
    be converted.

    Parameters
    ----------
    df : SparkDataFrame
        dataset to convert

    data_contract : List[Tuple[str, str, type]]
        data contract represented as a list of tuples of the following
        structure:
        (<original column name>, <new column name>, <data type>)

    drop_if_not_in_data_contract: bool
        if true, columns that are not in the 'data_contract' will be dropped

    Returns
    -------
    converted data
    """

    cols_original = df.columns

    cols = []

    for col_original, col_new, data_type in data_contract:
        cols.append(F.col(col_original).cast(data_type).alias(col_new))

    if drop_if_not_in_data_contract:
        return df.select(*cols)

    cols_data_contract = [x[0] for x in data_contract]
    cols_other = [x for x in df.columns if x not in cols_data_contract]
    cols = cols_other + cols
    df = df.select(*cols)
    return df


def col_from_map(
    col_name: str,
    val_mapping: Dict[str, Any],
    no_match_value: Optional[Any] = "unknown",
) -> Column:
    """
    Creates a column containing CASE/WHEN statement based on
    a python dictionary. Useful to rename labels in a DF

    Parameters
    ----------
    col_name : str
        column to be used that contains source values

    val_mapping : dict
        a dictionary of mapping between the source value and the new value

    no_match_value : Any
         in case of no-matches which value to put

    Returns
    -------
    a column in a Spark DF with new values
    """

    col = F

    for val_source, val_new in val_mapping.items():
        mask = F.col(col_name) == val_source
        col = col.when(mask, F.lit(val_new))

    col = col.otherwise(F.lit(no_match_value))

    return col


def left_anti_join_pdfs(
    pdf_left: pd.DataFrame,
    pdf_right: pd.DataFrame,
    on: List[str],
) -> pd.DataFrame:
    """
    Performs left-anti join between 2 pd.DataFrame's

    Parameters
    ----------
    pdf_left : pd.DataFrame
        dataframe on the left of the join

    pdf_right : pd.DataFrame
        dataframe on the left of the join

    on : List[str]
        list of column names that represent the join keys between 2 tables

    Returns
    -------
    the resulting of the left-anti join
    """
    # carrying out anti join using merge method
    pdf_merge = pdf_left.merge(pdf_right, on=on, how="left", indicator=True)
    pdf_filtered = pdf_merge.loc[pdf_merge["_merge"] == "left_only"]
    pdf_result = pdf_left[pdf_left[on].isin(pdf_filtered)]
    return pdf_result


def sample_random_value_from_list(list_: List[Union[str, int, float]]) -> SparkColumn:
    """
    Returns a random value in PySpark from a list of elements
    Used to assign values randomly to a column of a PySpark dataframe
    Parameters
    ----------
    list_: a list of elements

    Returns
    -------
    Random value generator for PySpark
    """
    options = [F.lit(x) for x in list_]
    return F.array(*options).getItem((F.rand() * len(options)).cast("int"))
