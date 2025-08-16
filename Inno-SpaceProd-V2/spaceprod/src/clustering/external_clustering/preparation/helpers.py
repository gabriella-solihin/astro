from typing import Any, Dict, List

import pandas as pd

from inno_utils.loggers import log
from pyspark.sql import SparkSession
from spaceprod.utils.data_helpers import strip_except_alpha_num, snake_case
from spaceprod.utils.imports import F, SparkDataFrame
from spaceprod.utils.validation import dup_check
from spaceprod.utils.names import get_col_names as cols


def preprocess_demographics(
    dfs: List[pd.DataFrame],
    dist_list: List[str],
    col_prefix: str,
    key: str,
):
    """function to process a list of demographic data.

    combined demographic data according to the list of demographic groups

    Parameters
    ----------
    dfs : list
        List of demographic datasets
    dist_list : list
        List of demographic groups
    col_prefix: str
        Environics data column prefix which is read from external clustering config
    key: str
        The primary key of demographic data
    Returns
    -------
    pd.DataFrame
        combined demographic data according to the list of demographic groups

    """

    log.info("preprocess the list of demographic data")
    df_demographic_lists = []
    # run through the list of demographic data within 5, 10, and 15 mins drive
    for suffix, df in zip(dist_list, dfs):
        df = df.T.drop_duplicates().T
        # only select 2020 demographic data fields
        d_cols = [c for c in df.columns if c.startswith(col_prefix)]
        df = df[[key] + d_cols]
        df.rename(columns={c: c + suffix for c in d_cols}, inplace=True)
        df_demographic_lists.append(df)
    # join the demographic data by the key
    demographic_df = pd.concat(
        [d.set_index(key) for d in df_demographic_lists], axis=1, join="inner"
    ).reset_index()
    return demographic_df


def preprocess_competitors(
    competitor_data_df: pd.DataFrame,
    competitor_group: List[str],
    banner_corrected_key: str,
    banner_corrected_format: str,
):
    """function to process competitor data

    preprocess competitor data according to the list of group attributes

    This function will create aggregation features including:
    1. number of competitor stores along each group
    2. preprocess the competitor data by adding # of mins drive time as suffix of data column names

    Parameters
    ----------
    competitor_data_df : pd.DataFrame
        Competitor dataframe
    group : list
        List of attributes for join operations
    banner_corrected_key : str
        Banner key to be updated with below banner corrected format
    banner_corrected_format : str
        Banner format for the given banner corrected key
    Returns
    -------
    pd.DataFrame
        processed competitor data according to the list of group attributes

    """

    log.info("preprocess competitor data")

    # add drive time column
    competitor_data_df["DRIVE_TIME"] = (
        competitor_data_df["DRIVE_TIME"]
        .apply(lambda x: str(x).replace("min", ""))
        .astype(int)
    )

    # preprocess column names and drop rows that doesn't have Comp_Parent_Co data field
    competitor_data_df_ = competitor_data_df.rename(
        columns={"COMP_BANNER_KEY": "BANNER_KEY"}
    )
    # this BK doesnt have any competitor however its requested by downstream to have the value
    competitor_data_df_.loc[
        competitor_data_df_["BK"] == "1011619", "COMP_PARENT_CO"
    ] = "OTHER"

    # reformatting the column to respected data types

    competitor_data_df_["COMP_BANNER_FORMAT"] = competitor_data_df_[
        "COMP_BANNER_FORMAT"
    ].apply(str)

    competitor_data_df_["COMP_BAN_TYPE"] = competitor_data_df_["COMP_BAN_TYPE"].apply(
        str
    )

    competitor_data_df_["COMP_PARENT_CO"] = competitor_data_df_["COMP_PARENT_CO"].apply(
        str
    )

    # convert column name as snake case and combining the columns
    competitor_data_df_["COMP_BAN_TYPE"] = (
        competitor_data_df_["COMP_PARENT_CO"].apply(lambda x: snake_case(x))
        + "_"
        + competitor_data_df_["COMP_BANNER_FORMAT"]
    )

    # Change COMP_BANNER_KEY 1016225 to COMP_BANNER_FORMAT "Discount"
    competitor_data_df_["COMP_BANNER_FORMAT"] = competitor_data_df_[
        "COMP_BANNER_FORMAT"
    ].apply(lambda x: snake_case(x, replace_string=" "))

    log.info("competitor data correction on 04/29/2021")
    # correction based on banner key
    competitor_data_df_.loc[
        competitor_data_df_["BANNER_KEY"] == banner_corrected_key,
        "COMP_BANNER_FORMAT",
    ] = banner_corrected_format

    competitor_data_df_.loc[
        competitor_data_df_["COMP_BANNER_FORMAT"] == "Community Service",
        "COMP_BANNER_FORMAT",
    ] = "Full Service"

    competitor_data_df_.loc[
        competitor_data_df_["COMP_BANNER_FORMAT"] == "Discount Service",
        "Comp_Banner_Format",
    ] = "Discount"

    # aggregation the number of competitor stores for the specified groups
    df_all = agg_df_cnt(
        df=competitor_data_df_,
        group=competitor_group,
        key="BANNER_KEY",
        prefix="Cmp_Store_Cnt_",
    )

    df_segment = agg_df_cnt(
        df=competitor_data_df_,
        group=competitor_group + ["COMP_BANNER_FORMAT"],
        key="BANNER_KEY",
        prefix="Cmp_Segment_",
    )
    df_banner_type_cnt = agg_df_cnt(
        df=competitor_data_df_,
        group=competitor_group + ["COMP_BAN_TYPE"],
        key="BANNER_KEY",
        prefix="Cmp_Ban_Type_",
    )
    df_banner_co_cnt = agg_df_cnt(
        df=competitor_data_df_,
        group=competitor_group + ["COMP_PARENT_CO"],
        key="BANNER_KEY",
        prefix="Cmp_Parent_Co_",
    )

    # join the dataframes based on the Bk attribute
    dfs = [df_all, df_segment, df_banner_type_cnt, df_banner_co_cnt]
    competitor_df = pd.concat(
        [d.set_index("BK") for d in dfs], axis=1, join="inner"
    ).reset_index()
    competitor_df.rename(columns={"BK": "BANNER_KEY"}, inplace=True)

    features = [
        "_".join(c.split("_")[:-1])
        for c in competitor_df.columns.to_list()
        if c.startswith("Cmp")
    ]
    competitor_df = competitor_df.fillna(0)

    # add minutes of drive time as suffix to the column names
    for f in features:
        for suffix in ["_5", "_10", "_15"]:
            if f + suffix not in competitor_df:
                competitor_df[f + suffix] = 0
        competitor_df[f + "_10"] = competitor_df[f + "_5"] + competitor_df[f + "_10"]
        competitor_df[f + "_15"] = competitor_df[f + "_10"] + competitor_df[f + "_15"]

    return competitor_df


def agg_df_cnt(
    df: pd.DataFrame, group: str, key: str, agg: str = "count", prefix: str = ""
):
    """function to aggregate dataframe according to the key.

    preprocess pandas dataframe according to the key and modify the column names by the prefix

    Parameters
    ----------
    df : pd.DataFrame
        dataframe to be aggregated
    group : str
        Group attribute to be grouped by
    key: str
        key attribute to be aggregated
    agg: str
        aggregation type
    prefix: str
        prefix string to be added to the column names
    Returns
    -------
    pd.DataFrame
        aggregated dataframe with modified column names using the provided prefix

    """

    # aggregation based on the group key
    df_agg = df.groupby(group)[key].agg(agg).reset_index()
    # unstacked key not starts with Bk
    unstack_cols = [c for c in group if not c.startswith("BK")]
    df_indexed: pd.DataFrame = df_agg.set_index(group).unstack(unstack_cols)
    # get all values of levels
    cols = df_indexed.columns.get_level_values(level=1)

    # get count of groups
    group_len = len(group)

    if group_len <= 2:
        # no need to flat the columns if group count is less than 2
        df_indexed.columns = [prefix + str(c) for c in cols]
    else:
        for i in range(2, group_len):
            # flat the columns
            curr_cols = df_indexed.columns.get_level_values(level=i)
            # convert the columns to camel case and add prefix to the column names
            cols = [
                prefix + strip_except_alpha_num(curr_cols[i]) + "_" + str(c)
                for i, c in enumerate(cols)
            ]
        df_indexed.columns = cols
    a = df_indexed.reset_index()
    return a


def process_store_data(
    store_list_df: pd.DataFrame,
    location_df: pd.DataFrame,
    filter_store_corrected: Dict[str, str],
    missing_ban_no: pd.DataFrame,
    update_missing_ban_no: pd.DataFrame,
):
    """function to process store data

    preprocess store data according to the location table

    Parameters
    ----------
    store_list_df : pd.DataFrame
        dataset contains list of stores
    location_df : pd.DataFrame
        dataset contains location information
    filter_store_corrected: Dict[str, str]
        Details of the store to be corrected
    missing_ban_no: Dict[str, str]
        dataset with missing store list based on postal code
    update_missing_ban_no: Dict[str, str]
        dataset containing ban nos of duplicated stores in same postal code

    Returns
    -------
    pd.DataFrame
        joined store data using location information

    """

    log.info("preprocess store data")
    location_df = location_df[
        [
            "STORE_NO",
            "STORE_PHYSICAL_LOCATION_NO",
            "REGION_DESC",
            "NATIONAL_BANNER_DESC",
            "POSTAL_CD",
        ]
    ].drop_duplicates()
    log.info("correction for store Sobeys Sutton (11429/7497)")

    # Updating a Store No for a postal cd N2Z2Y8
    location_df.loc[location_df["POSTAL_CD"] == "N2Z2Y8", "STORE_NO"] = "4108"
    location_df = location_df.append(filter_store_corrected, ignore_index=True)
    location_df.rename(columns={"STORE_NO": "BAN_NO"}, inplace=True)

    # mapping location to correct ban_no from missing store dict
    location_df_new = location_df.merge(missing_ban_no, on="POSTAL_CD", how="left")
    location_df_new["BAN_NO_CORRECTED"] = location_df_new["BAN_NO_CORRECTED"].fillna(
        location_df_new["BAN_NO"]
    )

    # removing the duplicated ban no for stores in same postal code
    location_df_final = location_df_new.merge(
        update_missing_ban_no, on="BAN_NO", how="left"
    )
    location_df_final["BAN_NO_FINAL"] = location_df_final["BAN_NO_FINAL"].fillna(
        location_df_final["BAN_NO_CORRECTED"]
    )

    location_df_final = location_df_final.drop("BAN_NO", axis=1)
    location_df_final.rename(columns={"BAN_NO_FINAL": "BAN_NO"}, inplace=True)
    store_list_df = store_list_df.dropna(subset=["BAN_NO"])
    location_df_final["BAN_NO"] = location_df_final["BAN_NO"].astype(int)
    store_list_df["BAN_NO"] = store_list_df["BAN_NO"].astype(int)
    store_list_df["BAN_DESC"] = store_list_df["BAN_DESC"].apply(lambda x: snake_case(x))
    store_list_df = store_list_df.merge(location_df_final, on="BAN_NO", how="inner")
    # rename the columns to title case as per the downstream requirement
    store_list_df.rename(
        columns={
            "REGION_DESC": cols().get("F_REGION_DESC"),
            "NATIONAL_BANNER_DESC": cols().get("F_BANNER"),
        },
        inplace=True,
    )
    store_list_df_selected = store_list_df[
        [
            "BANNER_KEY",
            "TOT_WKLY_SALES",
            "TOT_SALES_AREA",
            "TOT_GROSS_AREA",
            "STORE_PHYSICAL_LOCATION_NO",
            cols().get("F_REGION_DESC"),
            cols().get("F_BANNER"),
        ]
    ]

    store_list_df_selected.reset_index(inplace=True)
    return store_list_df_selected


def process_external_clustering_data(
    df: pd.DataFrame, spark: SparkSession, config: Dict[str, Any]
):
    """function to process external clustering data

    process external clustering data according to the configuration

    Parameters
    ----------
    df : pd.DataFrame
        external clustering dataset
    spark : SparkSession
        spark session for databricks cluster
    Returns
    -------
    pd.DataFrame
        processed external clustering data

    """

    if type(df) != pd.DataFrame:
        df = df.toPandas()

    df = df.loc[:, ~df.columns.duplicated()]

    # make column name in snake case to standardize column name formats for downstream pipelines
    df.rename(
        columns=dict(zip(df.columns, [strip_except_alpha_num(x) for x in df.columns])),
        inplace=True,
    )

    dropped_list = [
        "Banner_Key",
        "Province",
        "Ban_Desc",
        "Ban_Name",
        "Province",
        "index",
    ]
    cols = [col for col in df.columns if col not in dropped_list]

    # convert string to numeric values
    for c in cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df[["Banner_Key"] + cols]

    df_ = df.copy()

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
        if f.startswith(
            config["column"]["prefix"] + "_Household_Population_For_Total_Immigration"
        )
        if "other" in f.lower()
    ]
    excluded_cols_1 = (
        df_var[df_var["var"] == 0]["competitor_features"].to_list() + other_cols
    )
    df_ = df_[[col for col in df_.columns if col not in excluded_cols_1]]

    # Feature Denominator: to normalize the external features we calculate the features within each group
    # e.g., feature denominator of Korean minorities feature is population of all visible minority group
    path = config["external_clustering_inputs"]["inputs_location"]
    mapping = get_mapping_data(spark, config, path)
    # make column name in snake case
    mapping.rename(
        columns=dict(
            zip(mapping.columns, [strip_except_alpha_num(x) for x in mapping.columns])
        ),
        inplace=True,
    )

    map_dict = dict(zip(mapping["Feature"], mapping["Denominator"]))
    for col, denominator in map_dict.items():
        # col=config["column"]["prefix"]+"_"+col
        if denominator != "None":
            df_[col] = df_[col] / df_[denominator]

    sales_per_sqft = df_["Tot_Wkly_Sales"] / df_["Tot_Sales_Area"]

    tot_cols = [c for c in df_.columns if c.startswith("Tot_")]

    df_ = df_.fillna(0)
    df_.drop(["Banner_Key"] + tot_cols, axis=1, inplace=True)

    # step 2: calculate correlation
    corr_list = [df_[c].corr(sales_per_sqft) for c in df_.columns]
    return df_, corr_list


def clean_location(df_loc: SparkDataFrame):
    """
    This function adds the default value to STORE_NO column and selects the required columns

    Parameters
    ----------
    df: dataframe of location data

    Returns
    -------
    pd.DataFrame
        cleaned location data

    """
    df_loc = df_loc.na.drop(
        subset=[
            "RETAIL_OUTLET_LOCATION_SK",
            "STORE_PHYSICAL_LOCATION_NO",
            "STORE_NO",
            "REGION_DESC",
        ]
    )

    df_loc = df_loc.drop("VALID_FROM_DTTM", "VALID_TO_DTTM")
    # get the latest STORE_NO record for each store
    # the latest is the one that has the highest key (RETAIL_OUTLET_LOCATION_SK)
    df_latest_loc = df_loc.groupBy("STORE_NO").agg(
        F.max(F.col("RETAIL_OUTLET_LOCATION_SK")).alias("RETAIL_OUTLET_LOCATION_SK")
    )
    df_loc = df_loc.join(
        df_latest_loc, ["STORE_NO", "RETAIL_OUTLET_LOCATION_SK"], "inner"
    )

    dup_check(df_loc, ["STORE_NO"])

    return df_loc.toPandas()


def read_format_data_header(df: pd.DataFrame, isdemographic: bool, prefix: str):
    """
    This function cleans columns name, replaces unwanted words in the rows and drops columns generated based on the prefix for demographic data

    Parameters
    ----------
    df: dataframe of environics data

    isdemographic : bool
        Indicates if its a demographic data

    prefix: str
        prefix string to be added to the column names to generate dropping columns

    Returns
    -------
    pd.DataFrame
        cleaned environic data

    """
    prefix = prefix + "_"
    # removing special characters and replacing the " " or "__" with "_"
    df.columns = (
        df.columns.str.upper()
        .str.strip()
        .str.replace("[^0-9A-Za-z_ ]", "")
        .str.replace(" ", "_")
        .str.replace("__", "_")
    )
    # defining unwanted words in rows
    replace_words = ["NULL", "Null", "null", "N/A", "n/a", " "]
    df = replace_unwanted_string(df, replace_words)
    # removing duplicated columns for demographic data
    if isdemographic:
        df = df.drop([prefix + "TOTAL_POPULATION6"], axis=1)
        df = df.rename(
            columns={prefix + "TOTAL_POPULATION3": prefix + "TOTAL_POPULATION"}
        )
    return df


def replace_unwanted_string(df: pd.DataFrame, replace_words: List[str]):
    """
    This function replaces unwanted words in the environics data

    Parameters
    ----------
    df: dataframe of environics data

    group : List
        List of unwanted words

    Returns
    -------
    pd.DataFrame
        replaced environic data with 0 in place of unwanted word

    """
    # replacing unwnated words with 0
    for word in replace_words:
        df = df.replace(word, 0)
    return df


def get_mapping_data(df: pd.DataFrame, col_prefix: str):
    """
    This function replaces 2020 in the values of mapping.csv with the column prefix in the config

    Parameters
    ----------
    df: dataframe of mapping.csv

    Returns
    -------
    pd.DataFrame
        updated mapping.csv data

    """
    prefix = col_prefix + "_"
    return df.apply(lambda col: col.str.replace("2020_", prefix))


def combine_dataframe(
    df_demo: pd.DataFrame, df_comp: pd.DataFrame, df_store: pd.DataFrame
):
    """
    This function combines 3 dataframes and returns it as a list of dataframes

    Parameters
    ----------
    df_demo: dataframe of demographic data

    df_comp: dataframe of competitors data

    df_store: dataframe of store data

    Returns
    -------
    List[pd.DataFrame]
        List of above 3 dataframes

    """

    comb_dfs = [df_demo, df_comp, df_store]
    return comb_dfs


def final_patch_to_environics_data(
    df: SparkDataFrame, col_name_prefix: str
) -> SparkDataFrame:
    """
    This function does ad-hoc patches to make the output of environics module
    compatible with downstream modules
    TODO: ideally this needs to be addressed

    Parameters
    ----------
    df: output of environics pre-processing

    Returns
    -------

    """

    # drop the columns we are not sure where its coming from that is not
    # used downstream
    col_name = f"{col_name_prefix}_HOUSEHOLD_POPULATION_FOR_TOTAL_IMMIGRATION_BY_PLACE_OF_BIRTH_5"
    df = df.drop(col_name)
    return df
