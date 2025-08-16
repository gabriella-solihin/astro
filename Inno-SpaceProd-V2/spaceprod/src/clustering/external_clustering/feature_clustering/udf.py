from typing import List, Dict, Tuple

import numpy as np
import pandas as pd
from inno_utils.loggers import log

from spaceprod.utils.data_helpers import strip_except_alpha_num
from spaceprod.utils.names import get_col_names


def process_external_clustering_data(
    pdf_raw: pd.DataFrame, pdf_mapping: pd.DataFrame
) -> Tuple[pd.DataFrame, List]:
    """
    function to process external clustering data

    process external clustering data by dividing each columns with their respective
    denominators and calculate correlation w.r.t sales per square ft

    Parameters
    ----------
    pdf_raw : pd.DataFrame
        external clustering dataset
    pdf_mapping : pd.DataFrame
        external clustering dataset
    Returns
    -------
    Tuple[pd.DataFrame, List]
        processed external clustering data and list of correlation for each feature to sales per sq ft

    """
    # get column names
    n = get_col_names()

    # run clustering based on selected features from external clustering features
    key = n.F_BANNER_KEY

    pdf_raw = pdf_raw.loc[:, ~pdf_raw.columns.duplicated()]

    # make column name in snake case to standardize column name formats for downstream pipelines
    pdf_raw.rename(
        columns=dict(
            zip(pdf_raw.columns, [strip_except_alpha_num(x) for x in pdf_raw.columns])
        ),
        inplace=True,
    )

    pdf_mapping.rename(
        columns=dict(
            zip(
                pdf_mapping.columns,
                [strip_except_alpha_num(x) for x in pdf_mapping.columns],
            )
        ),
        inplace=True,
    )

    dropped_list = [
        key,
        "Province",
        "Ban_Desc",
        "Ban_Name",
        "index",
    ]
    cols = [col for col in pdf_raw.columns if col not in dropped_list]

    # convert string to numeric values
    for c in cols:
        pdf_raw[c] = pd.to_numeric(pdf_raw[c], errors="coerce")

    pdf_raw = pdf_raw[[key] + cols]

    df_ = pdf_raw.copy()

    # get variance of dataframe
    df_ = df_.fillna(0)
    df_var = (
        df_.var()
        .to_frame()
        .rename(columns={0: "var"})
        .rename_axis("competitor_features")
        .reset_index()
    )
    df_var = df_var.sort_values(by=["var"], ascending=True)

    # already have the majority of immigration composition in non-other columns
    other_cols = [
        f
        for f in df_.columns
        if f.startswith("2021_Household_Population_For_Total_Immigration")
        if "other" in f.lower()
    ]
    excluded_cols_1 = (
        df_var[df_var["var"] == 0]["competitor_features"].to_list() + other_cols
    )

    df_ = df_[[col for col in df_.columns if col not in excluded_cols_1]]

    # create dictionary feature denominator mapping and filter for only included features
    map_dict = dict(zip(pdf_mapping["Feature"], pdf_mapping["Denominator"]))
    for k in excluded_cols_1:
        map_dict.pop(k, None)

    for col, denominator in map_dict.items():
        if denominator != "None":
            df_[col] = df_[col] / df_[denominator]

    sales_per_sqft = df_["Tot_Wkly_Sales"] / df_["Tot_Sales_Area"]

    tot_cols = [c for c in df_.columns if c.startswith("Tot_")]

    df_ = df_.fillna(0)
    df_.drop(tot_cols, axis=1, inplace=True)

    # step 2: calculate correlation

    # drop non-feature columns
    # TODO: remove index when Anil confirms drop in data prep stage
    corr_drop_cols = [key, n.F_STORE_PHYS_NO, "Index"]
    df_calc_corr = df_.drop(corr_drop_cols, axis=1)

    # calculate correlations and create correlation dataframe
    corr_list = [df_calc_corr[c].corr(sales_per_sqft) for c in df_calc_corr.columns]
    df_corr = pd.DataFrame(
        zip(df_calc_corr.columns, corr_list), columns=["Feature", "Corr"]
    )

    return df_, df_corr


def normalized(df):
    """
    this function normalizes the df by ensure the values are numeric and then divides value - min by the max-min difference

    Parameters
    ----------
    df: pd.DataFrame
        table to write
    Returns
    --------
    df_normalized: pd.DataFrame
        normalized dataframe
    """

    df_ = df.copy()
    df_ = df_.apply(pd.to_numeric)
    df_ = df_.fillna(0)
    df_normalized = (df_ - df_.min()) / (df_.max() - df_.min())
    df_normalized = df_normalized.replace(np.nan, 0)
    return df_normalized


def feature_clustering_pre_proc_udf(
    input_data_raw: List[Dict[str, str]],
    input_data_mapping: List[Dict[str, str]],
):
    """
    TODO: Davin to add docstring
    Parameters
    ----------
    input_data_raw : List[Dict[str, str]]
        input to UDF in form of environics data
    input_data_mapping : List[Dict[str, str]]
        input to UDF in form of mapping from columns to their respective denominators

    Returns
    -------
    Tuple[Dict[str, float], Dict[str, float], List[float]]
        normalized dataframe, processed dataframe with denominators, correlation list of features w.r.t sales per sq ft
    """

    # get column names
    n = get_col_names()

    key = n.F_BANNER_KEY

    # convert the input lists to pandas DFs
    pdf_raw = pd.DataFrame.from_dict(input_data_raw)
    pdf_mapping = pd.DataFrame.from_dict(input_data_mapping)

    log.info("process the data")
    df_, df_corr = process_external_clustering_data(
        pdf_raw=pdf_raw,
        pdf_mapping=pdf_mapping,
    )

    # normalize data
    log.info("normalize the data")
    df_ = df_.set_index(key)
    df_selected = normalized(df=df_)
    df_selected[key] = df_selected.index

    # convert typing to fit into output schema
    df_selected[key] = df_selected[key].astype(float)
    df_corr = df_corr.astype(str)
    df_ = df_.astype(float)
    df_[key] = df_.index.astype(float)

    types_df_ = df_.dtypes.unique()
    types_df_selected = df_selected.dtypes.unique()
    types_corr_list = list(set([type(x) for x in df_corr]))

    msg = "Only floats allowed in df_"
    assert len(types_df_) == 1, msg
    assert types_df_[0] == np.float64, msg

    msg = "Only floats allowed in types_df_selected"
    assert len(types_df_selected) == 1, msg
    assert types_df_selected[0] == np.float64, msg

    msg = "Only string allowed in df_corr"
    assert len(types_corr_list) == 1, msg
    assert types_corr_list[0] == str, msg

    return (
        df_selected.to_dict("records"),
        df_.to_dict("records"),
        df_corr.to_dict("records"),
    )
