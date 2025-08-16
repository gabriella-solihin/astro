import traceback
from typing import Callable

import pandas as pd
import numpy as np

import pyspark.sql.types as T
from inno_utils.loggers.log import log
from pyspark.sql import DataFrame as SparkDataFrame

from spaceprod.src.optimization.post_process.helpers import (
    create_msg_for_udf_run_scope,
)
from spaceprod.utils.data_helpers import backup_on_blob
from spaceprod.utils.imports import F
from spaceprod.utils.space_context.spark import spark
import sys
from pyspark.sql import Window


def preprocess_removals(
    df_opt: SparkDataFrame, df_elas: SparkDataFrame, df_item_counts: SparkDataFrame
) -> SparkDataFrame:
    """
    Preprocess the input tables for removal, determining the difference in sales for an item's facing removed.

    Parameters
    ----------
    df_opt : optimization outputs
    df_elas : elasticity outputs
    df_item_counts : from elasticity preprocessing, contains the item counts for each item sold in every store

    Returns
    -------
    df_removal : SparkDataFrame
      joined table at the store-item level including columns such as the optimal facings and sales diff if we remove one facing
    """
    # 1. Join optimization, elasticity, and item counts together.
    # We do this to get each item's item (sales) counts, optimal facings, and elasticity curves.
    df_opt = df_opt.withColumnRenamed("Region_Desc", "REGION")
    df_elas = df_elas.withColumnRenamed("National_Banner_Desc", "BANNER")
    df_opt_elas = df_opt.join(
        df_elas,
        on=["REGION", "BANNER", "M_Cluster", "Section_Master", "Need_State", "ITEM_NO"],
        how="left",
    )
    df_opt_elas = df_opt_elas.select(
        "Region",
        "Banner",
        "STORE_CLUSTER_ID",
        "Item_No",
        "Section_Master",
        "Need_State",
        "Optim_Facings",
        *["facing_fit_" + str(i) for i in range(13)],
    )

    df_item_counts = df_item_counts.selectExpr(
        "STORE_CLUSTER_ID", "ITEM_NO", "Item_Count"
    )
    df_opt_elas = df_opt_elas.join(
        df_item_counts, on=["STORE_CLUSTER_ID", "ITEM_NO"], how="left"
    )

    # 2. Get the projected sales loss of removing 1 facing for each item.
    # This is done by getting the projected sales at optimal facings, projected sales at optimal facings - 1 (i.e. 1 facing removed), and then finding the difference in sales from removing that facing
    col_optim_facings = F.col("Optim_Facings").cast(T.IntegerType())
    col_optim_sales = F.col("facing_fit_list").getItem(col_optim_facings)

    col_facing_array = F.array(*[F.col("facing_fit_" + str(i)) for i in range(13)])
    col_optim_facings_one_less = F.col("Optim_Facings_One_Less").cast(T.IntegerType())
    col_optim_one_less_sales = F.col("facing_fit_list").getItem(
        col_optim_facings_one_less
    )

    df_removal = (
        df_opt_elas.filter(F.col("Optim_Facings") > 0)
        .filter(F.col("Optim_Facings") < 13)
        .withColumn("Optim_Facings_One_Less", F.col("Optim_Facings") - 1)
        .withColumn("facing_fit_list", col_facing_array)
        .drop(*["facing_fit_" + str(i) for i in range(13)])
        .withColumn("optim_sales", col_optim_sales)
        .withColumn("optim_one_less_sales", col_optim_one_less_sales)
        .withColumn("sales_diff", F.col("optim_sales") - F.col("optim_one_less_sales"))
    )

    return df_removal


def preprocess_macro_removals(
    df_opt: SparkDataFrame, df_elas: SparkDataFrame, df_item_counts: SparkDataFrame
) -> SparkDataFrame:
    """
    Preprocess the input tables for removal, determining the difference in sales for an item's facings removed.
    This function works very similarly in logic as preprocess_removals, but can be considered as a more generalized version
    in which we get the sales lift of removing all potential facings of an item, not just removing the first facing.

    Parameters
    ----------
    df_opt : optimization outputs
    df_elas : elasticity outputs
    df_item_counts : from elasticity preprocessing, contains the item counts for each item sold in every store

    Returns
    -------
    df_removal : SparkDataFrame
      joined table at the store-item level including columns such as the optimal facings and sales diff if we remove all facings
    """
    df_opt = df_opt.withColumnRenamed("Region_Desc", "REGION")
    df_elas = df_elas.withColumnRenamed("National_Banner_Desc", "BANNER")
    df_opt_elas = df_opt.join(
        df_elas,
        on=["REGION", "BANNER", "M_Cluster", "Section_Master", "Need_State", "ITEM_NO"],
        how="left",
    )
    df_opt_elas = df_opt_elas.select(
        "Region",
        "Banner",
        "STORE_CLUSTER_ID",
        "Item_No",
        "Section_Master",
        "Need_State",
        "Optim_Facings",
        "department",
        "m_cluster",
        *["facing_fit_" + str(i) for i in range(13)],
    )

    df_item_counts = df_item_counts.selectExpr(
        "STORE_CLUSTER_ID", "ITEM_NO", "Item_Count"
    )
    df_opt_elas = df_opt_elas.join(
        df_item_counts, on=["STORE_CLUSTER_ID", "ITEM_NO"], how="left"
    )

    # Find all the sales differences of every facing being removed (not just the sales diff of the first facing removed)
    # We do this to keep our UDF implementatios below to be very similar.
    # The list of differences in sales (one element in the list for each facing removed) is stored in a column of arrays called diff_sales_list
    max_facings = df_opt_elas.agg({"Optim_Facings": "max"}).collect()[0][0]
    diff_cols = [
        (F.col(f"facing_fit_{i}") - F.col(f"facing_fit_{i - 1}")).alias(
            f"sales_diff_{i}"
        )
        for i in range(1, max_facings + 1)
    ]
    diff_cols_array = F.array(*diff_cols)

    df_removal = (
        df_opt_elas.filter(F.col("Optim_Facings") > 0)
        .filter(F.col("Optim_Facings") < 13)
        .withColumn("diff_sales_list", diff_cols_array)
        .drop(*["facing_fit_" + str(i) for i in range(13)])
    )

    return df_removal


def preprocess_additions(
    df_opt: SparkDataFrame, df_elas: SparkDataFrame, df_item_counts: SparkDataFrame
) -> SparkDataFrame:
    """
    Preprocess the input tables for adding facings, determining the difference in sales for an item's facing added.
    This function works very similar as preprocess_removals, except here we find the incremental sales gain of potentially adding a facing instead of incremental sales loss of removing a facing.

    Parameters
    ----------
    df_opt : optimization outputs
    df_elas : elasticity outputs
    df_item_counts : from elasticity preprocessing, contains the item counts for each item sold in every store

    Returns
    -------
    df_add : SparkDataFrame
      joined table at the store-item level including columns such as the optimal facings and sales diff if we add one facing
    """
    df_opt = df_opt.withColumnRenamed("Region_Desc", "REGION")
    df_elas = df_elas.withColumnRenamed("National_Banner_Desc", "BANNER")
    df_opt_elas = df_opt.join(
        df_elas,
        on=["REGION", "BANNER", "M_Cluster", "Section_Master", "Need_State", "ITEM_NO"],
        how="left",
    )

    df_opt_elas = df_opt_elas.select(
        "Region",
        "Banner",
        "STORE_CLUSTER_ID",
        "Item_No",
        "Section_Master",
        "Need_State",
        "Optim_Facings",
        *["facing_fit_" + str(i) for i in range(13)],
    )

    df_item_counts = df_item_counts.selectExpr(
        "STORE_CLUSTER_ID", "ITEM_NO", "Item_Count"
    )
    df_opt_elas = df_opt_elas.join(
        df_item_counts, on=["STORE_CLUSTER_ID", "ITEM_NO"], how="left"
    )

    col_facing_fit_list = F.array(*[F.col("facing_fit_" + str(i)) for i in range(13)])
    col_optim_facings = F.col("Optim_Facings").cast(T.IntegerType())
    col_optim_facings_one_more = F.col("Optim_Facings_One_More").cast(T.IntegerType())
    col_optim_sales = F.col("facing_fit_list").getItem(col_optim_facings)
    col_optim_one_more_sales = F.col("facing_fit_list").getItem(
        col_optim_facings_one_more
    )

    df_add = df_opt_elas.filter(F.col("Optim_Facings") < 12)
    df_add = df_add.withColumn("Optim_Facings_One_More", F.col("Optim_Facings") + 1)
    df_add = df_add.withColumn("facing_fit_list", col_facing_fit_list)
    df_add = df_add.drop(*["facing_fit_" + str(i) for i in range(13)])
    df_add = df_add.withColumn("optim_sales", col_optim_sales)
    df_add = df_add.withColumn("optim_one_more_sales", col_optim_one_more_sales)
    df_add = df_add.withColumn(
        "sales_diff", F.col("optim_one_more_sales") - F.col("optim_sales")
    )

    return df_add


def preprocess_macro_additions(
    df_opt: SparkDataFrame, df_elas: SparkDataFrame, df_item_counts: SparkDataFrame
) -> SparkDataFrame:
    """
    Preprocess the input tables for addition, determining the difference in sales for an item's facings added.
    This function works very similar as preprocess_macro_removals, except here we find the incremental sales gain of potentially adding a facing instead of incremental sales loss of removing a facing.

    Parameters
    ----------
    df_opt : optimization outputs
    df_elas : elasticity outputs
    df_item_counts : from elasticity preprocessing, contains the item counts for each item sold in every store

    Returns
    -------
    df_addition : SparkDataFrame
      joined table at the store-item level including columns such as the optimal facings and sales diff if we add all facings (up to 12)
    """
    df_opt = df_opt.withColumnRenamed("Region_Desc", "REGION")
    df_elas = df_elas.withColumnRenamed("National_Banner_Desc", "BANNER")

    df_opt_elas = df_opt.join(
        other=df_elas,
        on=["REGION", "BANNER", "M_Cluster", "Section_Master", "Need_State", "ITEM_NO"],
        how="left",
    )

    df_opt_elas = df_opt_elas.select(
        "Region",
        "Banner",
        "STORE_CLUSTER_ID",
        "Item_No",
        "Section_Master",
        "Need_State",
        "Optim_Facings",
        "department",
        "m_cluster",
        *["facing_fit_" + str(i) for i in range(13)],
    )

    cols = ["STORE_CLUSTER_ID", "ITEM_NO", "Item_Count"]
    df_item_counts = df_item_counts.select(*cols)

    df_opt_elas = df_opt_elas.join(
        other=df_item_counts,
        on=["STORE_CLUSTER_ID", "ITEM_NO"],
        how="left",
    )

    diff_cols = [
        (F.col(f"facing_fit_{i}") - F.col(f"facing_fit_{i - 1}")).alias(
            f"sales_diff_{i}"
        )
        for i in range(1, 13)
    ]
    diff_cols_array = F.array(*diff_cols)

    df_addition = (
        df_opt_elas.filter(F.col("Optim_Facings") > 0)
        .filter(F.col("Optim_Facings") < 13)
        .withColumn("diff_sales_list", diff_cols_array)
        .drop(*["facing_fit_" + str(i) for i in range(13)])
    )

    return df_addition


def get_removal_rankings_udf() -> Callable:
    """
    Determine the rankings for removing facings within each store - section master.

    Parameters
    ----------
    df_removal : joined table at the store-item level including columns such as the optimal facings and sales diff if we remove one facing

    Returns
    -------
    df_output : SparkDataFrame
      order of removal for each item, within a store - section master
    """
    schema_out_ranking = T.StructType(
        [
            T.StructField("REGION", T.StringType(), True),
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("STORE_CLUSTER_ID", T.StringType(), True),
            T.StructField("SECTION_MASTER", T.StringType(), True),
            T.StructField("ITEM_TO_REMOVE", T.IntegerType(), True),
            T.StructField("ORDER", T.IntegerType(), True),
        ]
    )

    def get_min(all_items_ns: dict, applicable_need_states: set):
        """Get the least productive item remaining in all_items_ns that is eligible."""
        curr_min_diff = 1000000  # a big number

        curr_ns_min, curr_item_min, facings_min = 0, 0, 0

        # Go through all need states to find the least productive item across the need states.
        for ns, ns_items in all_items_ns.items():
            item_ref = ns_items[0][0]
            diff_ref = ns_items[0][1]
            facings_ref = ns_items[0][2]
            if (
                diff_ref < curr_min_diff and ns in applicable_need_states
            ):  # need to make sure that the NS we are removing from is still eligible.
                curr_ns_min = ns
                curr_item_min = item_ref
                facings_min = facings_ref
                curr_min_diff = diff_ref

        return curr_ns_min, curr_item_min, facings_min, curr_min_diff

    @F.pandas_udf(schema_out_ranking, F.PandasUDFType.GROUPED_MAP)
    def get_ranking_udf(df):
        region = df.Region.iloc[0]
        banner = df.Banner.iloc[0]
        store = df.STORE_CLUSTER_ID.iloc[0]
        sm = df.Section_Master.iloc[0]

        # TODO: Alan, for this UDF function:
        #   1. add comments inside function to explain each step
        #   2. add docstring & typing to `df` param. it probably should be `pdf`

        def get_items_in_ns(df: pd.DataFrame):
            """Returns a dictionary, keyed by NS, of all items sorted by their productivity (least productive item first)"""
            df = df.sort_values(
                ["sales_diff", "Item_Count"], ascending=[True, True]
            )  # tie break by sales item count
            return {
                "Need_State": df.Need_State.iloc[0],
                "Items": [
                    (item_no, sales_diff, facings)
                    for (item_no, sales_diff), facings in zip(
                        zip(df.Item_No, df.sales_diff), df.Optim_Facings
                    )
                ],
            }

        all_items_ns = df.groupby("Need_State").apply(get_items_in_ns)
        all_items_ns = {e["Need_State"]: e["Items"] for e in all_items_ns}

        applicable_need_states_final = set(all_items_ns.keys())
        applicable_need_states = applicable_need_states_final.copy()
        item_orders = []
        while all_items_ns:
            while applicable_need_states:
                ns_to_remove, item_to_remove, facings, min_diff = get_min(
                    all_items_ns, applicable_need_states
                )
                if (
                    len(all_items_ns[ns_to_remove]) == 1 and facings == 1
                ):  # NS is protected - remove from consideration.
                    del all_items_ns[ns_to_remove]
                    applicable_need_states_final.remove(ns_to_remove)
                    applicable_need_states.remove(ns_to_remove)
                else:  # NS is not protected
                    item_orders.append(item_to_remove)  # add item to removal list
                    applicable_need_states.remove(
                        ns_to_remove
                    )  # remove NS from round robin this round
                    all_items_ns[ns_to_remove].pop(
                        0
                    )  # remove item from consideration permanently
                    if not all_items_ns[
                        ns_to_remove
                    ]:  # remove section if NS is exhausted
                        del all_items_ns[ns_to_remove]
                        applicable_need_states_final.remove(ns_to_remove)
            applicable_need_states = applicable_need_states_final.copy()

        return pd.DataFrame(
            {
                "REGION": region,
                "BANNER": banner,
                "STORE_CLUSTER_ID": store,
                "SECTION_MASTER": sm,
                "ITEM_TO_REMOVE": pd.Series(item_orders).astype(int),
                "ORDER": range(1, len(item_orders) + 1),
            }
        )

    return get_ranking_udf


def get_macro_removal_rankings_udf() -> Callable:
    """
    Determine the rankings for removing facings within each store - section master.

    Parameters
    ----------
    df_removal : joined table at the store-item level including columns such as the optimal facings and sales diff if we remove one facing

    Returns
    -------
    df_output : SparkDataFrame
      order of removal for each item, within a store - section master
    """
    schema_out_ranking = T.StructType(
        [
            T.StructField("REGION", T.StringType(), True),
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("STORE_CLUSTER_ID", T.StringType(), True),
            T.StructField("SECTION_MASTER", T.StringType(), True),
            T.StructField("m_cluster", T.StringType(), True),
            T.StructField("department", T.StringType(), True),
            T.StructField("ITEM_TO_REMOVE", T.IntegerType(), True),
            T.StructField("INCREMENTAL_SALES", T.FloatType(), True),
            T.StructField("ORDER", T.IntegerType(), True),
        ]
    )

    def get_min(all_items_ns: dict, applicable_need_states: set):
        """Get the least productive item remaining in all_items_ns that is eligible."""

        curr_ns_min, curr_item_min, facings_min = 0, 0, 0

        curr_min_diff = 1000000
        # Go through all need states to find the least productive item across the need states.
        for ns, ns_items in all_items_ns.items():
            item_ref = ns_items[0][0]
            diff_ref = ns_items[0][1]
            facings_ref = ns_items[0][2]
            if diff_ref < curr_min_diff and ns in applicable_need_states:
                curr_ns_min = ns
                curr_item_min = item_ref
                facings_min = facings_ref
                curr_min_diff = diff_ref
        return curr_ns_min, curr_item_min, facings_min, curr_min_diff

    @F.pandas_udf(schema_out_ranking, F.PandasUDFType.GROUPED_MAP)
    def get_ranking_udf(df):
        region = df.Region.iloc[0]
        banner = df.Banner.iloc[0]
        store = df.STORE_CLUSTER_ID.iloc[0]
        sm = df.Section_Master.iloc[0]
        m_clus = df.m_cluster.iloc[0]
        dpt = df.department.iloc[0]

        def get_all_facings(val):
            """Gets all possible facings from 0 to the optimal number of facings"""
            return np.array(range(0, val))

        def get_all_diffs(df):
            """Gets the corresponding sales differences from removing all the facings"""
            facings = df["all_facings_possible"]
            diffs = df["diff_sales_list"]
            return np.array([diffs[f] for f in facings])

        def get_items_in_ns(df):
            """Returns a dictionary, keyed by NS, of all item-facings sorted by their productivity (least productive first)
            This function returns the exact same output format as the more simpler version in get_removal_rankings_udf. However, it will be much larger because now we can remove more than 1 facing per item."""

            # Create two columns of arrays for all the possible facings we can remove until, as well as their corresponding sales diffs.
            df["all_facings_possible"] = df["Optim_Facings"].apply(get_all_facings)
            df["diffs_sales_possible"] = df[
                ["diff_sales_list", "all_facings_possible"]
            ].apply(get_all_diffs, axis=1)
            df[[i for i in range(0, max(df["Optim_Facings"]))]] = df[
                "diffs_sales_possible"
            ].apply(pd.Series)

            # Now we melt the table to get it at the item-facing level.
            # This allows our output format to be consistent with the simpler version above, so that we do not have to change the removal logic in the loop below.
            df = df.melt(
                id_vars=[
                    "STORE_CLUSTER_ID",
                    "Item_No",
                    "Region",
                    "Banner",
                    "Section_Master",
                    "m_cluster",
                    "department",
                    "Need_State",
                    "Optim_Facings",
                    "Item_Count",
                ],
                value_vars=[i for i in range(0, max(df["Optim_Facings"]))],
                var_name="result_facing",
                value_name="diff_sales",
            )
            df = df[~df["diff_sales"].isna()]
            df = df.sort_values(["diff_sales", "Item_Count"], ascending=[True, True])

            return {
                "Need_State": df.Need_State.iloc[0],
                "Items": [
                    (item_no, sales_diff, facings + 1)
                    for (item_no, sales_diff), facings in zip(
                        zip(df.Item_No, df.diff_sales), df.result_facing
                    )
                ],
            }

        all_items_ns = df.groupby("Need_State").apply(get_items_in_ns)
        all_items_ns = {e["Need_State"]: e["Items"] for e in all_items_ns}

        applicable_need_states_final = set(all_items_ns.keys())
        applicable_need_states = applicable_need_states_final.copy()
        item_orders = []
        value_orders = []
        while all_items_ns:
            while applicable_need_states:
                ns_to_remove, item_to_remove, facings, min_diff = get_min(
                    all_items_ns, applicable_need_states
                )
                if (
                    len(all_items_ns[ns_to_remove]) == 1 and facings == 1
                ):  # NS is protected - remove from consideration.
                    del all_items_ns[ns_to_remove]
                    applicable_need_states_final.remove(ns_to_remove)
                    applicable_need_states.remove(ns_to_remove)
                else:  # NS is not protected
                    item_orders.append(item_to_remove)  # add item to removal list
                    value_orders.append(min_diff)  # add item's incremental sales lost
                    applicable_need_states.remove(
                        ns_to_remove
                    )  # remove NS from round robin this round
                    all_items_ns[ns_to_remove].pop(
                        0
                    )  # remove item from consideration permanently
                    if not all_items_ns[
                        ns_to_remove
                    ]:  # remove section if NS is exhausted
                        del all_items_ns[ns_to_remove]
                        applicable_need_states_final.remove(ns_to_remove)
            applicable_need_states = applicable_need_states_final.copy()

        return pd.DataFrame(
            {
                "REGION": region,
                "BANNER": banner,
                "STORE_CLUSTER_ID": store,
                "SECTION_MASTER": sm,
                "m_cluster": m_clus,
                "department": dpt,
                "ITEM_TO_REMOVE": pd.Series(item_orders).astype(int),
                "INCREMENTAL_SALES": pd.Series(value_orders).astype(float),
                "ORDER": range(1, len(item_orders) + 1),
            }
        )

    return get_ranking_udf


def get_addition_rankings_udf() -> Callable:
    """
    Determine the rankings for adding facings within each store - section master.

    Parameters
    ----------
    df_add : joined table at the store-item level including columns such as the optimal facings and sales diff if we add one facing

    Returns
    -------
    df_output : SparkDataFrame
      order of addition for each item, within a store - section master
    """
    schema_out_ranking = T.StructType(
        [
            T.StructField("REGION", T.StringType(), True),
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("STORE_CLUSTER_ID", T.StringType(), True),
            T.StructField("SECTION_MASTER", T.StringType(), True),
            T.StructField("ITEM_TO_ADD", T.IntegerType(), True),
            T.StructField("ORDER", T.IntegerType(), True),
        ]
    )

    def get_max(all_items_ns: dict, applicable_need_states: set):
        """Get the most productive item remaining in all_items_ns that is eligible."""

        curr_ns_min, curr_item_min = 0, 0

        curr_max_diff = -1
        for ns, ns_items in all_items_ns.items():
            item_ref = ns_items[0][0]
            diff_ref = ns_items[0][1]
            if diff_ref > curr_max_diff and ns in applicable_need_states:
                curr_ns_min = ns
                curr_item_min = item_ref
                curr_max_diff = diff_ref
        return curr_ns_min, curr_item_min, curr_max_diff

    @F.pandas_udf(schema_out_ranking, F.PandasUDFType.GROUPED_MAP)
    def get_ranking_udf(df):
        region = df.Region.iloc[0]
        banner = df.Banner.iloc[0]
        store = df.STORE_CLUSTER_ID.iloc[0]
        sm = df.Section_Master.iloc[0]

        def get_items_in_ns(df):
            """Returns a dictionary, keyed by NS, of all items sorted by their productivity (most productive item first)"""
            df = df.sort_values(
                ["sales_diff", "Item_Count"], ascending=[False, False]
            )  # break ties on sales item count
            return {
                "Need_State": df.Need_State.iloc[0],
                "Items": [
                    (item_no, sales_diff, facings)
                    for (item_no, sales_diff), facings in zip(
                        zip(df.Item_No, df.sales_diff), df.Optim_Facings
                    )
                ],
            }

        all_items_ns = df.groupby("Need_State").apply(get_items_in_ns)
        all_items_ns = {e["Need_State"]: e["Items"] for e in all_items_ns}

        applicable_need_states_final = set(all_items_ns.keys())
        applicable_need_states = applicable_need_states_final.copy()
        item_orders = []
        while all_items_ns:
            while applicable_need_states:
                ns_to_add, item_to_add, max_diff = get_max(
                    all_items_ns, applicable_need_states
                )

                # add item to removal list
                item_orders.append(item_to_add)

                # remove NS from round robin this round
                applicable_need_states.remove(ns_to_add)

                # remove item from consideration permanently
                all_items_ns[ns_to_add].pop(0)

                # remove section if NS is exhausted
                if not all_items_ns[ns_to_add]:
                    del all_items_ns[ns_to_add]
                    applicable_need_states_final.remove(ns_to_add)

            applicable_need_states = applicable_need_states_final.copy()

        return pd.DataFrame(
            {
                "REGION": region,
                "BANNER": banner,
                "STORE_CLUSTER_ID": store,
                "SECTION_MASTER": sm,
                "ITEM_TO_ADD": pd.Series(item_orders).astype(int),
                "ORDER": range(1, len(item_orders) + 1),
            }
        )

    return get_ranking_udf


def get_macro_addition_rankings_udf() -> Callable:
    """
    Determine the rankings for adding facings within each store - section master.

    Parameters
    ----------
    df_add : joined table at the store-item level including columns such as the optimal facings and sales diff if we add one facing

    Returns
    -------
    df_output : SparkDataFrame
      order of addition for each item, within a store - section master
    """

    def get_max(all_items_ns: dict, applicable_need_states: set):
        """Get the most productive item remaining in all_items_ns that is eligible."""

        curr_ns_min, curr_item_min = 0, 0

        curr_max_diff = -1
        for ns, ns_items in all_items_ns.items():
            item_ref = ns_items[0][0]
            diff_ref = ns_items[0][1]
            if diff_ref > curr_max_diff and ns in applicable_need_states:
                curr_ns_min = ns
                curr_item_min = item_ref
                curr_max_diff = diff_ref
        return curr_ns_min, curr_item_min, curr_max_diff

    def logic_to_wrap(df):
        region = df.Region.iloc[0]
        banner = df.Banner.iloc[0]
        store = df.STORE_CLUSTER_ID.iloc[0]
        sm = df.Section_Master.iloc[0]
        m_clus = df.m_cluster.iloc[0]
        dpt = df.department.iloc[0]

        # region='ontario'; banner='SOBEYS'; store='ONT-SBY-FRZ-2B-10D'; sm='Frozen Handhelds'
        # df = df_removal.filter(F.col("region")==region).filter(F.col("banner")==banner).filter(F.col("STORE_CLUSTER_ID")==store).filter(F.col("Section_Master")==sm).toPandas()
        # assert len(df[["Region", "Banner", "STORE_CLUSTER_ID", "Section_Master"]].drop_duplicates())==1

        # TODO: Alan, for this UDF function:
        #   1. add comments inside function to explain each step
        #   2. add docstring & typing to `df` param. it probably should be `pdf`
        def get_all_facings(val):
            return np.array(
                range(val, 12)
            )  # since there are only 12 diffs we can index into below.

        def get_all_diffs(df):
            """Gets the corresponding sales differences from removing all the facings"""
            facings = df["all_facings_possible"]
            diffs = df["diff_sales_list"]
            return np.array([diffs[f] for f in facings])

        def get_items_in_ns(df):

            # df = df.query("Need_State==11")
            """Returns a dictionary, keyed by NS, of all items sorted by their productivity (most productive item first).
            This function works very similarly as the same subfunction in get_macro_removal_rankings_udf."""

            df["all_facings_possible"] = df["Optim_Facings"].apply(get_all_facings)

            df["diffs_sales_possible"] = df[
                ["diff_sales_list", "all_facings_possible"]
            ].apply(get_all_diffs, axis=1)

            max_num_additional_facings = 12 - min(df.Optim_Facings)

            df[[i for i in range(1, max_num_additional_facings + 1)]] = df[
                "diffs_sales_possible"
            ].apply(
                pd.Series
            )  # i represents number of additional facings from where you are.

            df = df.melt(
                id_vars=[
                    "STORE_CLUSTER_ID",
                    "Item_No",
                    "Region",
                    "Banner",
                    "Section_Master",
                    "m_cluster",
                    "department",
                    "Need_State",
                    "Optim_Facings",
                    "Item_Count",
                ],
                value_vars=[i for i in range(1, max_num_additional_facings + 1)],
                var_name="result_facing",
                value_name="diff_sales",
            )

            df["result_facing"] = df["result_facing"] + df["Optim_Facings"]

            df = df[~df["diff_sales"].isna()]

            df = df.sort_values(["diff_sales", "Item_Count"], ascending=[False, False])

            return {
                "Need_State": df.Need_State.iloc[0],
                "Items": [
                    (item_no, sales_diff, facings)
                    for (item_no, sales_diff), facings in zip(
                        zip(df.Item_No, df.diff_sales), df.result_facing
                    )
                ],
            }

        all_items_ns = df.groupby("Need_State").apply(get_items_in_ns)
        all_items_ns = {e["Need_State"]: e["Items"] for e in all_items_ns}

        applicable_need_states_final = set(all_items_ns.keys())
        applicable_need_states = applicable_need_states_final.copy()
        item_orders = []
        value_orders = []
        while all_items_ns:
            while applicable_need_states:
                ns_to_add, item_to_add, max_diff = get_max(
                    all_items_ns, applicable_need_states
                )

                # add item to removal list
                item_orders.append(item_to_add)
                value_orders.append(max_diff)

                # remove NS from round robin this round
                applicable_need_states.remove(ns_to_add)

                # remove item from consideration permanently
                all_items_ns[ns_to_add].pop(0)

                # remove section if NS is exhausted
                if not all_items_ns[ns_to_add]:
                    del all_items_ns[ns_to_add]
                    applicable_need_states_final.remove(ns_to_add)

            applicable_need_states = applicable_need_states_final.copy()

        return pd.DataFrame(
            {
                "REGION": region,
                "BANNER": banner,
                "STORE_CLUSTER_ID": store,
                "SECTION_MASTER": sm,
                "m_cluster": m_clus,
                "department": dpt,
                "ITEM_TO_ADD": pd.Series(item_orders).astype(int),
                "INCREMENTAL_SALES": pd.Series(value_orders).astype(float),
                "ORDER": range(1, len(item_orders) + 1),
            }
        )

    schema_out_ranking = T.StructType(
        [
            T.StructField("REGION", T.StringType(), True),
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("STORE_CLUSTER_ID", T.StringType(), True),
            T.StructField("SECTION_MASTER", T.StringType(), True),
            T.StructField("m_cluster", T.StringType(), True),
            T.StructField("department", T.StringType(), True),
            T.StructField("ITEM_TO_ADD", T.IntegerType(), True),
            T.StructField("INCREMENTAL_SALES", T.FloatType(), True),
            T.StructField("ORDER", T.IntegerType(), True),
        ]
    )

    @F.pandas_udf(schema_out_ranking, F.PandasUDFType.GROUPED_MAP)
    def udf_wrapper(pdf):

        cols = ["Region", "Banner", "STORE_CLUSTER_ID", "Section_Master"]
        msg_scope = create_msg_for_udf_run_scope(pdf[cols])
        log.info(f"Start running create_and_solve_model for: {msg_scope}")

        try:
            return logic_to_wrap(pdf)
        except Exception as ex:
            tb = traceback.format_exc()

            msg = f"""
            The task broke for scope: {msg_scope} 
            execution threw an error:\n{tb}
            \nFailed to run task for scope: {msg_scope} (see traceback above).
            """

            log.info(msg)

            raise Exception(msg)

    return udf_wrapper


def call_ranking_udf(df_add: SparkDataFrame, udf: Callable) -> SparkDataFrame:
    df_output = df_add.groupBy(
        ["REGION", "BANNER", "STORE_CLUSTER_ID", "SECTION_MASTER"]
    ).apply(udf)

    df_output = backup_on_blob(spark, df_output)

    return df_output


def call_ranking_udf_macro(df_add: SparkDataFrame, udf: Callable) -> SparkDataFrame:
    df_output = df_add.groupBy(
        [
            "REGION",
            "BANNER",
            "STORE_CLUSTER_ID",
            "SECTION_MASTER",
            "m_cluster",
            "department",
        ]
    ).apply(udf)

    df_output = backup_on_blob(spark, df_output)

    return df_output


def create_store_cluster_id_item_counts(
    df_bay_data_pre_index: SparkDataFrame,
    df_opt_output_with_store_cluster_id: SparkDataFrame,
) -> SparkDataFrame:
    """
    Crates item count summary using elasticity and opt outputs

    TODO: evaluate if taking the 'mode' aggregation here is suitable

    Parameters
    ----------
    df_bay_data_pre_index : SparkDataFrame
        elasticity model output to be aggregated

    df_opt_output_with_store_cluster_id : SparkDataFrame
        opt model output on store_cluster_id level, used to get the mapping
        between store number and store_cluster_id

    Returns
    -------

    """
    df_elas = df_bay_data_pre_index
    df_opt = df_opt_output_with_store_cluster_id

    ren = ["store_physical_location_id", "STORE_PHYSICAL_LOCATION_NO"]
    df_opt = df_opt.withColumnRenamed(*ren)

    cols = ["STORE_PHYSICAL_LOCATION_NO", "ITEM_NO", "Item_Count"]
    df_elas = df_elas.select(*cols)

    df_elas = df_elas.drop("Item_Count")

    df_result = df_elas.join(
        other=df_opt,
        on=["STORE_PHYSICAL_LOCATION_NO", "ITEM_NO"],
        how="inner",
    )

    cols = ["STORE_CLUSTER_ID", "ITEM_NO", "Item_Count"]
    df_result = df_result.select(*cols)

    agg = [F.sum("Item_Count").cast(T.IntegerType()).alias("Item_Count")]
    dims = ["STORE_CLUSTER_ID", "ITEM_NO"]
    df_result = df_result.groupBy(dims).agg(*agg)
    df_result = df_result.withColumnRenamed("store_cluster_id", "STORE_CLUSTER_ID")

    return df_result


def rank_section_macro_removal(
    df_micro_ranking: SparkDataFrame, df_opt: SparkDataFrame
):
    """Rank each section by increasing producitivity, if we were to remove one legal section break."""
    df_micro_ranking = df_micro_ranking.withColumnRenamed("ITEM_TO_REMOVE", "ITEM_NO")
    df_micro_ranking_widths = df_micro_ranking.join(
        df_opt.select("STORE_CLUSTER_ID", "ITEM_NO", "WIDTH", "LIN_SPACE_PER_BREAK"),
        on=["STORE_CLUSTER_ID", "ITEM_NO"],
    )

    dims = [
        "REGION",
        "BANNER",
        "m_cluster",
        "STORE_CLUSTER_ID",
        "department",
        "SECTION_MASTER",
    ]
    w = Window.partitionBy(dims).orderBy(["ORDER"])
    df_micro_ranking_widths = df_micro_ranking_widths.withColumn(
        "WIDTH_CUM_SUM", F.sum("WIDTH").over(w.rowsBetween(-sys.maxsize, 0))
    )
    df_one_door = df_micro_ranking_widths.filter(
        F.col("WIDTH_CUM_SUM") < F.col("LIN_SPACE_PER_BREAK")
    )
    df_macro = df_one_door.groupBy(dims).agg(
        F.sum("INCREMENTAL_SALES").alias("ONE_DOOR_SALES")
    )

    w = Window.partitionBy(["REGION", "BANNER", "m_cluster", "department"]).orderBy(
        ["ONE_DOOR_SALES"]
    )
    df_macro = df_macro.withColumn("SECTION_REMOVAL_ORDER", F.rank().over(w))
    return df_macro


def rank_section_macro_addition(
    df_micro_ranking: SparkDataFrame, df_opt: SparkDataFrame
):
    """Rank each section by decreasing producitivity, if we were to add one legal section break."""
    df_micro_ranking = df_micro_ranking.withColumnRenamed("ITEM_TO_ADD", "ITEM_NO")
    df_micro_ranking_widths = df_micro_ranking.join(
        df_opt.select("STORE_CLUSTER_ID", "ITEM_NO", "WIDTH", "LIN_SPACE_PER_BREAK"),
        on=["STORE_CLUSTER_ID", "ITEM_NO"],
    )

    dims = [
        "REGION",
        "BANNER",
        "m_cluster",
        "STORE_CLUSTER_ID",
        "department",
        "SECTION_MASTER",
    ]
    w = Window.partitionBy(dims).orderBy(["ORDER"])
    df_micro_ranking_widths = df_micro_ranking_widths.withColumn(
        "WIDTH_CUM_SUM", F.sum("WIDTH").over(w.rowsBetween(-sys.maxsize, 0))
    )
    df_one_door = df_micro_ranking_widths.filter(
        F.col("WIDTH_CUM_SUM") < F.col("LIN_SPACE_PER_BREAK")
    )
    df_macro = df_one_door.groupBy(dims).agg(
        F.sum("INCREMENTAL_SALES").alias("ONE_DOOR_SALES")
    )

    w = Window.partitionBy(["REGION", "BANNER", "m_cluster", "department"]).orderBy(
        F.col("ONE_DOOR_SALES").desc()
    )
    df_macro = df_macro.withColumn("SECTION_ADDITION_ORDER", F.rank().over(w))
    return df_macro
