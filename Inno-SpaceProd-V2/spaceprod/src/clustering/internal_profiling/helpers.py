from typing import List, Tuple

import numpy as np
import pandas as pd

from inno_utils.azure import write_blob
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def output_concatenated_need_states(
    spark,
    need_states_read: SparkDataFrame,
    product: SparkDataFrame,
    internal_need_state_out_path: str,
) -> None:
    """this just outputs the concatenated output including all need states with special ones (veggies)

    Parameters
    ----------
    spark: SparkSession
    need_states_read: SparkDataFrame
      dataframe that has transactions to add sales information to dashboard output
    product: SparkDataFrame
        dataframe that has the summary of need state assignments
    internal_output_path: str
        path where to output dataframe

    Returns
    -------
        outputs to blob that are 1) by need state level of sales
    """
    # temp only for dashboard info
    temp_need = need_states_read.join(
        product.drop(
            "lvl4_name",
            "lvl2_id",
            "lvl3_id",
            "lvl2_name",
            "lvl5_name",
            "item_name",
            "lvl3_name",
            "category_id",
            "item_sk",
            "lvl5_id",
        ),
        on=["ITEM_NO"],
        how="left",
    ).dropDuplicates(subset=["ITEM_NO"])

    write_blob(
        spark,
        temp_need,
        f"{internal_need_state_out_path}need_state_output_all_categories.csv",
        file_format="csv",
    )


def output_transaction_summary_by_section(
    trx_processed: SparkDataFrame,
    need_states_read: SparkDataFrame,
) -> Tuple[SparkDataFrame, SparkDataFrame]:
    """for dashboard only. - create transaction sale summary per section and need states

    Parameters
    ----------
    spark: SparkSession
    trx_processed: SparkDataFrame
      dataframe that has transactions to add sales information to dashboard output
    need_states_read: SparkDataFrame
        dataframe that has the summary of need state assignments
    internal_output_path: str
        path where to output dataframe



    Returns
    -------
        outputs to blob that are 1) by section / category and 2) by need state level of sales
    """
    # drop duplicates as they are in both need_states_read as well as trx_processed
    trx_processed = trx_processed.drop(
        "lvl4_name",
        "lvl2_id",
        "lvl3_id",
        "lvl2_name",
        "lvl5_name",
        "item_name",
        "lvl3_name",
        "category_id",
        "item_sk",
        "lvl5_id",
    )

    df = (
        trx_processed.groupBy("REGION", "BANNER", "ITEM_NO")
        .agg(F.sum(F.col("SELLING_RETAIL_AMT")).alias("Sales"))
        .join(need_states_read, on=["REGION", "BANNER", "ITEM_NO"], how="inner")
    )

    dims_section = ["REGION", "BANNER", "POG_SECTION"]
    dims_need_state = dims_section + ["NEED_STATE"]
    agg = [F.sum(F.col("Sales")).alias("Sales")]

    sale_summary_section = df.groupBy(*dims_section).agg(*agg)
    sale_summary_need_state = df.groupBy(*dims_need_state).agg(*agg)

    return sale_summary_section, sale_summary_need_state


def create_profiler_dfs_for_dashboard(
    df_clustering_output_assignment: SparkDataFrame,
    df_combined_sales_store_section: SparkDataFrame,
    df_combined_sales_levels: SparkDataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """this function is used for tableau dashboard. it created the dataframe to add total sales
    by category and NS to the results

    Parameters
    ----------
    result_df: pd.DataFrame
      dataframe that has all the results of the clustering assignments
    combined_sales_store_section : pd.DataFrame
        sales by store and section
    combined_sales_levels: pd.DataFrame
        sales by need states only

    Returns
    --------
    profile_section_level: pd.DataFrame
        dataframe of proportions by category level
    profile_section_NS_level: pd.DataFrame
        dataframe of proportions by category and need state level
    """
    # do section level first - we merge cluster results here

    pdf_clustering = df_clustering_output_assignment.toPandas()
    pdf_combined_sales_store_section = df_combined_sales_store_section.toPandas()
    pdf_combined_sales_levels = df_combined_sales_levels.toPandas()

    selection = pdf_clustering[
        ["REGION", "BANNER", "STORE_PHYSICAL_LOCATION_NO", "store_cluster"]
    ]

    profile_section_level = pdf_combined_sales_store_section.merge(
        selection, on="STORE_PHYSICAL_LOCATION_NO", how="left"
    ).sort_values("store_cluster")

    # do not select region and banner because the downstream joined table will have it.
    selection = pdf_clustering[["STORE_PHYSICAL_LOCATION_NO", "store_cluster"]]
    profile_section_NS_level = pdf_combined_sales_levels.merge(
        selection, on="STORE_PHYSICAL_LOCATION_NO", how="left"
    ).sort_values("store_cluster")

    return profile_section_level, profile_section_NS_level


def create_mean(df, levels, levels_last_ones, col: str = "prop_qty") -> pd.DataFrame:
    """this function created mean index used in tableau dashboard to indicate whether
    a category is over a clusters average or not

    Parameters
    ----------
    df: pd.DataFrame
      dataframe that has the sales per need state for each store and the total summary
    levels : list
        top levels to aggregate on e.g. "store_cluster", "pog_section", "need_state"
    levels_last_ones: list
        last ones to aggregate on e.g. "pog_section", "need_state"
    col: str
        col of interest e.g. NS prop

    Returns
    --------
    profile_combined: pd.DataFrame
        dataframe of the categories / features with avg. index and standard deviation scores
    """

    # col to normalize store "size" by qty should be section_prop instead of prop_qty
    # mean calculations used for dashboard
    # calculate mean of the col in question
    profile_section_level_agg = (
        df.pivot_table(index=levels, values=[col], aggfunc=np.mean)
        .reset_index()
        .rename({col: "prop_qty_mean"}, axis=1)
    )
    #
    profile_section_level_agg_grand_mean_across_clusters = (
        df.pivot_table(index=levels_last_ones, values=[col], aggfunc=np.mean)
        .reset_index()
        .rename({col: "prop_qty_grand_mean"}, axis=1)
    )

    profile_combined = profile_section_level_agg.merge(
        profile_section_level_agg_grand_mean_across_clusters,
        on=levels_last_ones,
        how="left",
    )
    profile_combined["Index"] = (
        profile_combined["prop_qty_mean"] / profile_combined["prop_qty_grand_mean"]
    ) * 100
    profile_combined = profile_combined.drop(
        ["prop_qty_mean", "prop_qty_grand_mean"], axis=1
    )

    return profile_combined


def create_std_dev(
    df: pd.DataFrame,
    levels: List[str],
    levels_last_ones: List[str],
    col: str = "prop_qty",
) -> pd.DataFrame:
    """this function created standard deviation index used in tableau dashboard to indicate whether
    a category is over a clusters average or not

    Parameters
    ----------
    df: pd.DataFrame
      dataframe that has the sales per need state for each store and the total summary
    levels : list
        top levels to aggregate on e.g. "store_cluster", "pog_section", "need_state"
    levels_last_ones: list
        last ones to aggregate on e.g. "pog_section", "need_state"
    col: str
        col of interest e.g. NS prop

    Returns
    --------
    profile_combined: pd.DataFrame
        dataframe of the categories / features with avg. index and standard deviation scores
    """

    # same for std deviation used for dashboard
    profile_section_level_agg = (
        df.pivot_table(index=levels, values=[col], aggfunc=np.std)
        .reset_index()
        .rename({col: "prop_qty_stdev"}, axis=1)
    )

    profile_section_level_agg_grand_mean_across_clusters = (
        df.pivot_table(index=levels_last_ones, values=[col], aggfunc=np.std)
        .reset_index()
        .rename({col: "prop_qty_grand_stdev"}, axis=1)
    )

    profile_combined = profile_section_level_agg.merge(
        profile_section_level_agg_grand_mean_across_clusters,
        on=levels_last_ones,
        how="left",
    )
    profile_combined["Index_stdev"] = (
        profile_combined["prop_qty_stdev"] / profile_combined["prop_qty_grand_stdev"]
    ) * 100
    profile_combined = profile_combined.drop(
        ["prop_qty_stdev", "prop_qty_grand_stdev"], axis=1
    )

    return profile_combined


def run_dashboard_index_and_std_dev_outputs(
    spark: SparkSession, level: str, profile_df: pd.DataFrame
):
    """This function takes the proportions and store cluster assignment and calculates
    the index & st. dev. at different levels

    Parameters
    ----------
    spark: SparkSession
    internal_output_path: str
      dataframe that has the sales per need state for each store and the total summary
    level : str
        number of clusters we want to generate
    profile_df: pd.DataFrame

    """

    if level == "section_level":
        # create final table section level
        levels = [
            "REGION",
            "BANNER",
            "store_cluster",
            "POG_SECTION",
        ]
        levels_last_ones = ["REGION", "BANNER", "POG_SECTION"]
        col_of_interest = "section_prop"
    elif level == "section_NS_level":  # level should be "section_NS_level"
        levels = ["REGION", "BANNER", "store_cluster", "POG_SECTION", "NEED_STATE"]
        levels_last_ones = ["REGION", "BANNER", "POG_SECTION", "NEED_STATE"]
        col_of_interest = "NS_prop"
    else:
        raise Exception(f"illegal 'level' arg: {level}")

    # we first calculate the mean index on specific level (either section or section NS)
    profile_temp = create_mean(
        profile_df, levels, levels_last_ones, col=col_of_interest
    )

    # we then calculate the st. deviation at the specific level
    profile_temp_std = create_std_dev(
        profile_df, levels, levels_last_ones, col=col_of_interest
    )
    # we merge index and std. dev together
    profile_final_section_level = profile_temp.merge(
        profile_temp_std, on=levels, how="left"
    )

    df = spark.createDataFrame(profile_final_section_level)

    return df
