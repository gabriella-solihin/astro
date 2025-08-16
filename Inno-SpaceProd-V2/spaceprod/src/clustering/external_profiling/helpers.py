from functools import reduce
from operator import and_
from typing import Dict, List

import numpy as np
import pandas as pd

from inno_utils.loggers import log
from pyspark.sql import DataFrame as SparkDataFrame
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.data_helpers import strip_except_alpha_num
from spaceprod.utils.data_transformation import df_to_dict, is_col_null_mask
from spaceprod.utils.imports import F


def concat_features(
    df: pd.DataFrame,
    var_list: List[str],
    group: str,
    cluster_label: str = "Cluster_Labels",
    key: str = "Store_Physical_Location_No",
    measure: str = "count",
) -> pd.DataFrame:
    """Creates summarized column names to concat the features together by store

    Parameters
    ___________
    df: SparkDataFrame
        this is the dataframe with the transactions
    var_list: List
        the cluster label that we group the transactions by
    group: str
        cluster_label: List
        the cluster label that we group the transactions by
    key: str
        key str to concatenate by - should be stores if not given
    measure: str
        count or mean or std. dev


    Returns
    -------
    df_merged: pd.DataFrame
      DataFrame with the renamed features by store

    """

    df_list = []
    # this loop concatenates the column names with the count of the feature so that they are easier read
    for i, var in enumerate(var_list):
        curr_df = df[[cluster_label, key, var]]
        curr_df = curr_df.rename(columns={var: measure})
        curr_df[group] = var
        df_list.append(curr_df)

    df_merged = pd.concat(df_list)
    df_merged.rename(columns={group: "var_details"}, inplace=True)
    df_merged["var"] = group

    return df_merged


def get_profile_summary(df: SparkDataFrame, cluster_label: List[str]) -> SparkDataFrame:
    """Creates a summary of visits, item quantity, spend, spend per visit,
      promo spend and promo spend per visit

    Parameters
    ___________
    df: SparkDataFrame
        this is the dataframe with the transactions
    cluster_label: List
        the cluster label that we group the transactions by

    Returns
    -------
    cluster_summary: pyspark.sql.dataframe.DataFrame
      DataFrame containing the summary

    """

    # Get overall summary
    cluster_summary = df.groupBy(*cluster_label).agg(
        F.countDistinct("TRANSACTION_RK").alias("TOT_VISITS"),
        F.sum("ITEM_QTY").alias("TOT_UNITS"),
        F.sum("SELLING_RETAIL_AMT").alias("TOT_SALES"),
        F.sum("PROMO_SALES").alias("TOT_PROMO_SALES"),
        F.sum("PROMO_UNITS").alias("TOT_PROMO_UNITS"),
    )

    # Calculate per visit and per unit metrics
    cluster_summary = cluster_summary.withColumn(
        "SPEND_PER_VISIT", F.col("TOT_SALES") / F.col("TOT_VISITS")
    )
    cluster_summary = cluster_summary.withColumn(
        "UNITS_PER_VISIT", F.col("TOT_UNITS") / F.col("TOT_VISITS")
    )
    cluster_summary = cluster_summary.withColumn(
        "PRICE_PER_UNIT", F.col("TOT_SALES") / F.col("TOT_UNITS")
    )

    # Calculate percentage of sales and units on promo
    cluster_summary = cluster_summary.withColumn(
        "PERC_PROMO_UNITS", F.col("TOT_PROMO_UNITS") / F.col("TOT_UNITS")
    )
    cluster_summary = cluster_summary.withColumn(
        "PERC_PROMO_SALES", F.col("TOT_PROMO_SALES") / F.col("TOT_SALES")
    )

    # Calculate percentage of total sales, visits and units
    # could be refactored with F.lit here in phase 2
    tot_visit_count = df.agg(F.countDistinct("TRANSACTION_RK")).collect()[0][0]
    tot_units_sold = df.agg(F.sum("ITEM_QTY")).collect()[0][0]
    tot_sales = df.agg(F.sum("SELLING_RETAIL_AMT")).collect()[0][0]

    cluster_summary = cluster_summary.withColumn(
        "PERC_TOTAL_SALES", F.col("TOT_SALES") / tot_sales
    )
    cluster_summary = cluster_summary.withColumn(
        "PERC_TOTAL_UNITS", F.col("TOT_UNITS") / tot_units_sold
    )
    cluster_summary = cluster_summary.withColumn(
        "PERC_TOTAL_VISITS", F.col("TOT_VISITS") / tot_visit_count
    )
    return cluster_summary


def calculate_profiling_statistics(
    df: pd.DataFrame,
    levels: List[str],
    levels_last_ones: List[str],
    col: str = "prop_qty",
    measure: str = "mean",
) -> pd.DataFrame:
    """col to normalize store "size" by qty should be cannib_prop instead of prop_qty
    mean calculations used for dashboard
    calculate mean of the col in question

     Parameters
     ___________
     df: SparkDataFrame
         this is the dataframe with the transactions
    levels : list
        top levels to aggregate on e.g. "store_cluster", "cannib_id", "need_state"
    levels_last_ones: list
        last ones to aggregate on e.g. "cannib_id", "need_state"
    col: str
        col of interest e.g. NS prop
    measure: str
        mean or std. dev.

     Returns
    --------
    profile_combined: pd.DataFrame
        profiling dataframe used in dashbaord
    """

    if measure == "mean":
        agg_func = np.mean
        agg_col = "Index"
        suffix = "_mean"

    elif measure == "std":
        agg_func = np.std
        agg_col = "Index_Stdev"
        suffix = "_stdev"
    else:
        raise Exception(f"Invalid 'measure' value: '{measure}'")

    col_name = f"{col}{suffix}"
    col_name_grand = f"{col}_grand{suffix}"

    # now we aggregate by the agg func to get the total across the categories or need states
    profile_cannib_level_agg = (
        df.pivot_table(index=levels, values=[col], aggfunc=agg_func, dropna=True)
        .reset_index()
        .rename({col: col_name}, axis=1)
    )

    # in case of the data is empty, it will NOT create the column, need to
    # create an emtpy column manually
    if col_name not in profile_cannib_level_agg.columns:
        # we allow to do this patch only when the df is empty becasue of the
        # behaviour of .pivot_table
        assert len(profile_cannib_level_agg) == 0
        profile_cannib_level_agg[col_name] = float(0)

    # we now aggregate on the first two columns (items and need state for example) to ensure we get the totals and the grand mean
    profile_cannib_level_agg_grand_mean_across_clusters = df.pivot_table(
        index=levels_last_ones, values=[col], aggfunc=agg_func, dropna=True
    ).reset_index()

    # in case of the data is empty, it will NOT create the column, need to
    # create an emtpy column manually
    if col not in profile_cannib_level_agg_grand_mean_across_clusters.columns:
        # we allow to do this patch only when the df is empty becasue of the
        # behaviour of .pivot_table
        assert len(profile_cannib_level_agg_grand_mean_across_clusters) == 0
        profile_cannib_level_agg_grand_mean_across_clusters[col] = float(0)

    # the grand mean is now created to later divide by to get proportions
    profile_cannib_level_agg_grand_mean_across_clusters = (
        profile_cannib_level_agg_grand_mean_across_clusters.rename(
            {col: col_name_grand}, axis=1
        )
    )

    profile_combined = profile_cannib_level_agg.merge(
        right=profile_cannib_level_agg_grand_mean_across_clusters,
        on=levels_last_ones,
        how="left",
    )

    profile_combined[agg_col] = (
        profile_combined[col_name] / profile_combined[col_name_grand]
    ) * 100

    profile_combined = profile_combined.drop([col_name, col_name_grand], axis=1)

    profile_combined.sort_values(agg_col, ascending=False)

    return profile_combined


def filter_features_10_mins_away(df: SparkDataFrame) -> List:
    """
    filtering columns with the required distance

    Parameters
    ----------
    df : SparkDataFrame
     dataframe from which the necessary columns are to be filtered

    Returns
    -------
    df_columns : List
    list of filtered columns x mins away
    """

    df_columns = df.columns
    for dist in ["_10"]:
        df_columns = [c for c in df_columns if c.endswith(dist)]

    return df_columns


def group_features(features: List, key: str) -> List:
    """
    group features based on the key

    Parameters
    ----------
    features : List
        columns of a dataframe
    key : str
        grouping features based on the string e.g. Africa, Asia etc.

    Returns
    -------
    List
        list of set of columns based on the key passed
    """

    curr_list = [f for f in features if f.startswith(key)]
    return list(set(curr_list))


def update_immigration_group(
    value, features, exp
):  ### prefix in config - use this to refer
    """
    update the year in prefix of the terms/variables
    Parameters
    ----------
    value : list
        list of country groups passed from the config yaml
    features : list
        columns
    exp :  str
        this value is obtained from the external profiling config yaml

    Returns
    -------
    list of updated features
    """
    prefix = f"{exp}_Household_Population_For_Total_Immigration_By_Place_Of_Birth_"
    features_new = [
        "_".join(f.split("_"))
        for f in features
        if any([v in f for v in value]) and f.startswith(prefix)
    ]
    return list(set(features_new))


def gather_external_clustering_data(
    df_ext_clust_prof: SparkDataFrame,
    df_ext_clust_output: SparkDataFrame,
):
    """
    converts spark dataframe outputs of external clustering module to pandas

    Parameters
    ----------
    df_ext_clust_prof : SparkDataFrame
        this dataframe is used for profiling
    df_ext_clust_output : SparkDataFrame
        this dataframe is the output of external clustering module

    Returns
    -------
    pandas dataframe of both clustering and profiling datasets
    """
    # shortcuts
    df_clust = df_ext_clust_output  # old name: df (has more columns)
    df_prof = df_ext_clust_prof  # old name: df_demo (has less columns)

    pdf_clust = df_clust.toPandas()
    pdf_prof = df_prof.toPandas()

    # rename columns to match snake case
    pdf_clust.rename(
        columns=dict(
            zip(
                pdf_clust.columns,
                [strip_except_alpha_num(x) for x in pdf_clust.columns],
            )
        ),
        inplace=True,
    )

    # rename columns to match snake case
    pdf_prof.rename(
        columns=dict(
            zip(pdf_prof.columns, [strip_except_alpha_num(x) for x in pdf_prof.columns])
        ),
        inplace=True,
    )

    return pdf_clust, pdf_prof


def create_feature_groupings(
    cols_10_mins: List[str],
    africa_grp: List[str],
    asia_grp: List[str],
    europe_grp: List[str],
    latin_america_grp: List[str],
    feature_list: List[str],
    exp: List[str],
):
    """
    groups features into immigration, location, demo features, competitive features etc. based on the
    list defined in the config

    Parameters
    ----------
    cols_10_mins : str
        based on the column values, they are grouped into various groups
    africa_grp : list
        countries in the African continent - list defined in config
    asia_grp : list
        countries in the Asian continent - list defined in config
    europe_grp : list
        countries in the European continent - list defined in config
    latin_america_grp : list
        countries in the Lat-Am continent - list defined in config
    feature_list : list
        groups
    exp : str
        year as defined in the config

    Returns
    -------
    grouped columns into logical grouping of demography, competitors etc.
    """

    income_features = group_features(cols_10_mins, f"{exp}_Household_Income_")
    cmp_segment_features = group_features(cols_10_mins, "Cmp_Segment")
    cmp_banner_type_features = group_features(cols_10_mins, "Cmp_Ban_Type")
    minorities_features = group_features(
        cols_10_mins, f"{exp}_Total_Visible_Minorities_"
    )
    population_features = group_features(cols_10_mins, f"{exp}_Total_Population_")
    education_features = group_features(
        cols_10_mins,
        f"{exp}_Household_Population_15_Years_Or_Over_For_Educational_Attainment_",
    )

    africa_immigrant_group = update_immigration_group(africa_grp, cols_10_mins, exp)
    asia_immigrant_group = update_immigration_group(asia_grp, cols_10_mins, exp)
    europe_immigrant_group = update_immigration_group(europe_grp, cols_10_mins, exp)
    latin_america_immigrant_group = update_immigration_group(
        latin_america_grp, cols_10_mins, exp
    )

    immigrant_group = (
        africa_immigrant_group
        + asia_immigrant_group
        + europe_immigrant_group
        + latin_america_immigrant_group
    )

    other_cmp_features = [
        c
        for c in group_features(cols_10_mins, "Cmp_")
        if c not in cmp_segment_features + cmp_banner_type_features
    ]

    demo_features = (
        minorities_features
        + immigrant_group
        + population_features
        + income_features
        + education_features
    )

    other_demo_features = [
        c for c in group_features(cols_10_mins, f"{exp}_") if c not in demo_features
    ]

    features = [
        africa_immigrant_group,
        asia_immigrant_group,
        europe_immigrant_group,
        latin_america_immigrant_group,
        minorities_features,
        population_features,
        income_features,
        other_demo_features,
        education_features,
        cmp_segment_features,
        cmp_banner_type_features,
        other_cmp_features,
    ]

    feature_dict = dict(zip(feature_list, features))

    return feature_dict


def create_cmp_non_cmp_data(feature_dict, pdf_clust, pdf_prof):
    """
    groups features into competitive and non competitive features

    Parameters
    ----------
    feature_dict : dict
        feature dictionary
    pdf_clust : pandas dataframe
        clustering output
    pdf_prof : pandas dataframe
        profiling features dataframe

    Returns
    -------
    features grouped into competitive and non-competitive feature
    """

    dfs = []
    for key, item in feature_dict.items():
        curr_df = pd.DataFrame(data={"var": key, "var_details": item})
        dfs.append(curr_df)

    feature_dict = pd.concat(dfs)
    feature_dict = feature_dict.rename(
        {"VAR": "var", "VAR_DETAILS": "var_details"}, axis=1
    )

    # for each group which has unique var / features concatenate them
    groups = feature_dict["var"].unique()
    # initiate empty list to fill iteration over
    profiles = []
    for key in groups:
        var_list = list(feature_dict[feature_dict["var"] == key]["var_details"])
        if "competitor" in key.lower():
            df_ = concat_features(pdf_clust, var_list, key)
        else:
            df_ = concat_features(pdf_prof, var_list, key)

        profiles.append(df_)

    # apend final profiles to profile_level
    profile_level = pd.concat(profiles)
    profile_level["count"] = profile_level["count"].astype("float")

    # define the competitor columns by seeing which ones start with Cmp
    cmp_cols = list(
        set([c for c in profile_level["var_details"] if c.startswith("Cmp_")])
    )

    # filter on only the var details that are in competitor columns
    cmp_df = profile_level.loc[profile_level["var_details"].isin(cmp_cols), :]

    # define the negative set of the competitor to find all others
    non_cmp_df = profile_level.loc[~profile_level["var_details"].isin(cmp_cols), :]

    return cmp_df, non_cmp_df


def create_df_profiling(cmp_df, non_cmp_df) -> pd.DataFrame:
    """
    profiles features by calculating mean and standard deviation by cluster. Also concatenates dataframes

    Parameters
    ----------

    cmp_df : dataframe
        dataframe with competition features
    non_cmp_df : dataframe
        dataframe with non-competition features

    Returns
    -------
    spark dataframe with the profiling features
    """

    levels = ["Cluster_Labels", "var", "var_details"]
    levels_last_ones = ["var", "var_details"]

    profiling_lt = []
    # for the competitor cols and non comp cols calculate the mean of the counts for the statistics
    for df_comp in [cmp_df, non_cmp_df]:
        mean_df = calculate_profiling_statistics(
            df=df_comp,
            levels=levels,
            levels_last_ones=levels_last_ones,
            col="count",
            measure="mean",
        )

        std_df = calculate_profiling_statistics(
            df=df_comp,
            levels=levels,
            levels_last_ones=levels_last_ones,
            col="count",
            measure="std",
        )

        profiling_lt.append(mean_df.merge(right=std_df, on=levels, how="left"))

    final_profiling_df = pd.concat(profiling_lt)

    # output the final profiling dataframe
    return final_profiling_df


def exclude_invalid_region_banners(pdf_profiling: pd.DataFrame) -> SparkDataFrame:
    """
    Filters REGION/BANNERs to exclude invalid combinations
    TODO: investigate why some region/banner combinations don't get any
     statistics (nulls) see 'calculate_profiling_statistics' functions
     meanwhile we are filtering those out

    Parameters
    ----------
    pdf_profiling: final output of external profiling

    Returns
    -------
    filtered output of external profiling
    """

    # shortcut
    df = spark.createDataFrame(pdf_profiling)

    mask_1 = ~(is_col_null_mask("Index_Stdev"))
    mask_2 = ~(is_col_null_mask("Index"))
    mask = mask_1 & mask_2

    df_filtered = df.filter(mask)
    cols = ["REGION", "BANNER"]
    df_before = df.select(*cols).dropDuplicates()
    df_after = df_filtered.select(*cols).dropDuplicates()
    df_dropped = df_before.join(df_after, cols, "left_anti")
    pdf_dropped = df_dropped.toPandas()

    msg = f"""
    The following REGION/BANNER combinations were
    excluded because they had no statistics returned
    see TODO in this function:
    spaceprod.src.clustering.profiling_utils.exclude_invalid_region_banners    
    {pdf_dropped}
    """

    if len(pdf_dropped) > 0:
        log.info(msg)
    else:
        log.info("No REGION/BANNER combinations dropped")

    return df_filtered


def get_common_region_banner(df_ext_clust_prof, df_ext_clust_output):
    """
    gets common region and banner combination between the profiling and clustering datasets

    Parameters
    ----------
    df_ext_clust_prof : SparkDataFrame
        output of external clustering used for profiling
    df_ext_clust_output : SparkDataFrame
        output of external clustering

    Returns
    -------
    finds the common banner and region permutations between profiling and clustering dataframes

    """
    # find common region/banner combinations from both datasets
    # to iterate over them
    dims = ["Region_Desc", "Banner"]
    df_prof_dims = df_ext_clust_prof.select(*dims).dropDuplicates()
    df_output_dims = df_ext_clust_output.select(*dims).dropDuplicates()
    df_common_dims = df_prof_dims.join(df_output_dims, dims, "inner")
    permutations = df_to_dict(df_common_dims)
    log.info(f"Got {len(permutations)} region/banner permutations")

    return permutations


def region_banner_profiling(
    permutations: List[Dict[str, str]],
    df_ext_clust_prof: SparkDataFrame,
    df_ext_clust_output: SparkDataFrame,
    africa_grp: List[str],
    asia_grp: List[str],
    europe_grp: List[str],
    latin_america_grp: List[str],
    feature_list: List[str],
    exp: List[str],
) -> pd.DataFrame:
    """
    it is the main function that applies business logic by calling other sub functions to profile
    competition and non-competition features

    Parameters
    ----------
    permutations : dict
        common banner and region permutations between profiling and clustering dataframes
    df_ext_clust_prof : SparkDataFrame
        output of external clustering module used for profiling
    df_ext_clust_output : SparkDataFrame
        output of external clustering module

    Returns
    -------
    profiling information for region banner store - with mean and standard deviation information
    """

    ###########################################################################
    #  OBTAIN REQUIRED CONFIG PARAMETERS
    ###########################################################################

    # collect results here
    list_dfs = []

    # iterate through all available permutations of region/banner
    for perm in permutations:
        # perm = {'Region_Desc': 'Ontario', 'Banner': 'SOBEYS'}
        region = perm["Region_Desc"]
        banner = perm["Banner"]

        msg = f"Start processing profiling for region='{region}'; banner='{banner}'"
        log.info(msg)

        list_masks = [
            F.col("Region_Desc") == region,
            F.col("Banner") == banner,
        ]

        # filter both inputs
        mask = reduce(and_, list_masks)
        df_ext_clust_prof_perm = df_ext_clust_prof.filter(mask)
        df_ext_clust_output_perm = df_ext_clust_output.filter(mask)

        pdf_clust, pdf_prof = gather_external_clustering_data(
            df_ext_clust_prof=df_ext_clust_prof_perm,
            df_ext_clust_output=df_ext_clust_output_perm,
        )

        msg = f"Got ext clustering data for region='{region}'; banner='{banner}'"
        log.info(msg)

        # create feature_dict dataframe
        cols_10_mins = filter_features_10_mins_away(pdf_clust)

        feature_dict = create_feature_groupings(
            cols_10_mins=cols_10_mins,
            africa_grp=africa_grp,
            asia_grp=asia_grp,
            europe_grp=europe_grp,
            latin_america_grp=latin_america_grp,
            feature_list=feature_list,
            exp=exp,
        )

        cmp_df, non_cmp_df = create_cmp_non_cmp_data(
            feature_dict=feature_dict,
            pdf_clust=pdf_clust,
            pdf_prof=pdf_prof,
        )

        pdf_profiling_perm = create_df_profiling(cmp_df, non_cmp_df)

        # specify region/banner dimensions for data
        pdf_profiling_perm["REGION"] = region
        pdf_profiling_perm["BANNER"] = banner

        # append to the list of results
        list_dfs.append(pdf_profiling_perm)

    # combine all region/banner specific
    pdf_profiling = pd.concat(list_dfs)

    return pdf_profiling
