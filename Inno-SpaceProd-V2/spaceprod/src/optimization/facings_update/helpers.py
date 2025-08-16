from functools import reduce
from operator import and_
from typing import List

from inno_utils.loggers.log import log
from spaceprod.src.utils import dedup_location, dedup_product
from spaceprod.utils.data_transformation import is_col_null_mask, union_dfs
from spaceprod.utils.imports import F, SparkDataFrame, T
from spaceprod.utils.validation import dup_check
from spaceprod.utils.names import ColumnNames, get_col_names

import pandas as pd
import numpy as np


def add_col_region_banner_store(
    df_pog: SparkDataFrame,
    df_loc: SparkDataFrame,
) -> SparkDataFrame:
    """
    Adds  region, banner, store information to the 'combined_pog_processed'
    data

    Parameters
    ----------
    df_pog : SparkDataFrame
        'combined_pog_processed' data

    df_loc : SparkDataFrame
        location data

    Returns
    -------
    'combined_pog_processed' data with the new column(s) added
    """

    df_loc = dedup_location(df_loc, "STORE_NO")

    cols = [
        F.col("STORE_NO").alias("STORE"),
        F.col("NATIONAL_BANNER_DESC").alias("BANNER"),
        F.lower(F.col("REGION_DESC")).alias("REGION"),
        F.col("STORE_PHYSICAL_LOCATION_NO"),
    ]

    df_loc = df_loc.select(*cols)
    dup_check(df_loc, ["STORE"])

    df_pog = df_pog.join(df_loc, "STORE", "left")
    df_pog = df_pog.dropDuplicates(subset=["STORE_PHYSICAL_LOCATION_NO", "ITEM_NO"])
    # dup_check(df_pog, ["STORE_PHYSICAL_LOCATION_NO", "ITEM_NO"])

    return df_pog


def add_col_dept(df_pog: SparkDataFrame, df_dep: SparkDataFrame) -> SparkDataFrame:
    """
    Adds  region, banner, store information to the 'combined_pog_processed'
    data

    Parameters
    ----------
    df_pog : SparkDataFrame
        'combined_pog_processed' data

    df_dep : SparkDataFrame
        'department_mapping' file

    Returns
    -------
    'combined_pog_processed' data with the new column(s) added
    """

    # quick dup check before we start joining
    dims = ["REGION", "BANNER", "SECTION_MASTER"]
    dup_check(df_dep, dims)

    # select only required cols
    cols = dims + ["DEPARTMENT"]
    df_dep = df_dep.select(*cols)

    # perform value corrections
    # pog_department_mapping_file always has upper case region, needs to be
    # corrected for compatibility
    df_dep = df_dep.withColumn("REGION", F.lower(F.col("REGION")))
    df_dep = df_dep.withColumn("BANNER", F.upper(F.col("BANNER")))

    # perform the lookup
    df_pog = df_pog.join(df_dep, dims, "left")

    return df_pog


def filter_to_expected_scope(
    df_pog: SparkDataFrame,
    regions: List[str],
    banners: List[str],
    depts: List[str],
    pog_section_masters: List[str],
) -> SparkDataFrame:
    """
    Filters the  'combined_pog_processed' data to the expected scope that
    is driven by the scope yml file

    Parameters
    ----------
    df_pog : SparkDataFrame
        'combined_pog_processed' data

    regions : List[str]
        list of regions in scope from scope config

    banners : List[str]
        list of banners in scope from scope config

    depts : List[str]
        list of depts in scope from scope config

    pog_section_masters : List[str]
        list of SMs to include

    Returns
    -------
    'combined_pog_processed' data filtered to the right scope
    """

    msg_scope = f"\nregions: {regions}\nbanners: {banners}\ndepts: {depts}"
    msg = f"\nFiltering 'Combined POG Processed' to the expected scope:{msg_scope}"
    log.info(msg)

    list_mask = []

    if len(regions) > 0:
        log.info(f"Filtering to regions: {regions}")
        list_mask += [F.col("REGION").isin(regions)]

    if len(banners) > 0:
        log.info(f"Filtering to depts: {banners}")
        list_mask += [F.col("BANNER").isin(banners)]

    if len(depts) > 0:
        log.info(f"Filtering to depts: {depts}")
        list_mask += [F.col("DEPARTMENT").isin(depts)]

    if len(pog_section_masters) > 0:
        log.info(f"Filtering to SM's: {pog_section_masters}")
        list_mask += [F.col("SECTION_MASTER").isin(pog_section_masters)]

    if len(list_mask) > 0:
        mask = reduce(and_, list_mask)
        df = df_pog.filter(mask)
    else:
        df = df_pog

    # check that we did not filter everything
    msg = f"\nAfter scope filter, no pog data left, check your scope:{msg_scope}"
    assert df.limit(1).count() > 0, msg

    return df


def determine_missing_store_sm_combinations(
    df_pog: SparkDataFrame, df_opt: SparkDataFrame
) -> SparkDataFrame:
    """
    Finds missing store/SM combinations which are missing from the opt output

    Parameters
    ----------
    df_pog : SparkDataFrame
         'combined_pog_processed' data

    df_opt : SparkDataFrame
         optimization output

    Returns
    -------
    only the slice of 'combined_pog_processed' data that is missing
    the store/SM combinations in opt output
    """

    dims = ["STORE_PHYSICAL_LOCATION_NO", "SECTION_MASTER"]
    df_expected = df_pog.select(*dims).dropDuplicates()
    df_existing = df_opt.select(*dims).dropDuplicates()
    df_missing = df_expected.join(df_existing, dims, "left_anti")

    # start building out a new "slice" of optimization output for missing
    # data
    df_opt_missing = df_pog.join(df_missing, dims, "inner")

    return df_opt_missing


def add_col_need_state(
    df_opt_missing: SparkDataFrame,
    df_fns: SparkDataFrame,
) -> SparkDataFrame:
    """
    Adds need state information to the 'combined_pog_processed' data

    Parameters
    ----------
    df_opt_missing : SparkDataFrame
         'combined_pog_processed' data slice for missing store/POG combinations

    df_fns : SparkDataFrame
         'final_need_states' data from NS module output

    Returns
    -------
    'combined_pog_processed' data with the new column(s) added
    """

    # shortcut
    df = df_opt_missing

    dims = [
        "REGION",
        F.col("NATIONAL_BANNER_DESC").alias("BANNER"),
        "SECTION_MASTER",
        "ITEM_NO",
    ]
    dup_check(df_fns, dims)

    cols = dims + ["NEED_STATE"]
    df_fns = df_fns.select(cols)
    dims = ["REGION", "BANNER", "SECTION_MASTER", "ITEM_NO"]
    df = df.join(df_fns, dims, "left")

    # if there was no match here (i.e. this combination was dropped in
    # NS module), we set this to NS=-2 to indicate that
    mask = is_col_null_mask("NEED_STATE")
    col = F.when(mask, F.lit("-2")).otherwise(F.col("NEED_STATE"))
    df = df.withColumn("NEED_STATE", col)

    # log the information on how many store/SM records are not found in NS
    n_not_in_ns = df.filter(F.col("NEED_STATE") == "-2").count()
    log.info(f"Number of store/SM combinations not found in NS: {n_not_in_ns}")

    return df


def add_col_merged_cluster(
    df_opt_missing: SparkDataFrame,
    df_mci: SparkDataFrame,
    df_mce: SparkDataFrame,
    use_merged_clusters_post_review: bool,
) -> SparkDataFrame:
    """
    Adds merged cluster information to the 'combined_pog_processed' data

    Parameters
    ----------
    df_opt_missing : SparkDataFrame
      'combined_pog_processed' data slice for missing store/POG combinations

    df_mci : SparkDataFrame
      'merged_cluster' data from clustering module

    df_mce : SparkDataFrame
      'merged_clusters_external' data that was revised by users (external)

    use_merged_clusters_post_review : bool
        True if we want to use external MC data

    Returns
    -------
    'combined_pog_processed' data with the new column(s) added
    """

    # shortcut
    df = df_opt_missing

    # select the correct MC data
    df_mc = df_mce if use_merged_clusters_post_review else df_mci
    msg = f"Using EXTERNAL merged clusters (post-review): {use_merged_clusters_post_review}"
    log.info(msg)

    # quick dup check before joining
    dup_check(df_mc, ["STORE_PHYSICAL_LOCATION_NO"])

    if use_merged_clusters_post_review:
        df_mc = df_mc.withColumnRenamed("MERGE_CLUSTER", "MERGED_CLUSTER")

    # lookup the "MERGED_CLUSTER"
    df_mc = df_mc.select("STORE_PHYSICAL_LOCATION_NO", "MERGED_CLUSTER")
    df = df.join(df_mc, ["STORE_PHYSICAL_LOCATION_NO"], "left")

    # if there was no match here (i.e. this combination was dropped in
    # clustering), we set this to MC=-2 to indicate that
    mask = is_col_null_mask("MERGED_CLUSTER")
    col = F.when(mask, F.lit("-2")).otherwise(F.col("MERGED_CLUSTER"))
    df = df.withColumn("MERGED_CLUSTER", col)

    # log the information on how many store/SM records are not found in MC
    n_not_in_mc = df.filter(F.col("MERGED_CLUSTER") == "-2").count()
    log.info(f"Number of store/SM combinations not found in MC: {n_not_in_mc}")

    return df


def add_col_prod_info(
    df_opt_missing: SparkDataFrame, df_prd: SparkDataFrame
) -> SparkDataFrame:
    """
    Adds product information to the 'combined_pog_processed' data

    Parameters
    ----------
    df_opt_missing : SparkDataFrame
         'combined_pog_processed' data slice for missing store/POG combinations

    df_prd : SparkDataFrame
         'product' table (external)

    Returns
    -------
    'combined_pog_processed' data with the new column(s) added
    """
    df_prd = dedup_product(df_prd, ["ITEM_NO"])

    cols = [
        "ITEM_NO",
        "LVL4_NAME",
        "ITEM_NAME",
    ]

    df_prd = df_prd.select(*cols)
    dup_check(df_prd, ["ITEM_NO"])
    df = df_opt_missing.join(df_prd, "ITEM_NO", "left")

    return df


def combine_opt_and_missing_opt(
    df_opt_missing: SparkDataFrame,
    df_opt: SparkDataFrame,
) -> SparkDataFrame:
    """
    Combines (unions) together the optimization output and the
    slice of data that is missing optimization output for which we
    fall back to the CURRENT facings

    Parameters
    ----------
    df_opt_missing : SparkDataFrame
         'combined_pog_processed' data slice for missing store/POG combinations

    df_opt : SparkDataFrame
         optimization output

    Returns
    -------
    optimization output with missing store/SM combinations
    """

    # shortcut
    df_m = df_opt_missing
    df_o = df_opt

    # first set the required values in the missing slice
    cols_missing_slice = [
        F.col("REGION").alias("REGION"),
        F.col("BANNER").alias("Banner"),
        F.col("M_Cluster").alias("M_Cluster"),  # from MC
        F.col("SECTION_MASTER").alias("Section_Master"),
        F.col("ITEM_NO").alias("Item_No"),
        F.col("STORE_PHYSICAL_LOCATION_NO").alias("Store_Physical_Location_No"),
        F.col("LVL4_NAME").alias("Lvl4_Name"),  # from product
        F.col("NEED_STATE").cast(T.StringType()).alias("Need_State"),  # from final NS
        F.col("NEED_STATE").cast(T.StringType()).alias("Need_State_Idx"),  # TODO
        F.col("ITEM_NAME").alias("Item_Name"),  # from product
        F.col("WIDTH").cast(T.DoubleType()).alias("Width"),
        F.lit(False).alias("Constant_Treatment"),
        F.col("FACINGS").cast(T.DoubleType()).alias("Current_Facings"),
        F.col("FACINGS").cast(T.DoubleType()).alias("Optim_Facings"),
        F.col("Opt_Sales").cast(T.DoubleType()).alias("Cur_Sales"),
        F.col("Opt_Sales").cast(T.DoubleType()).alias("Opt_Sales"),
        F.col("Item_Count").cast(T.DoubleType()).alias("Item_Count"),
        F.lit(-1).cast(T.LongType()).alias("Unique_Items_Cur_Assorted"),
        F.lit(-1).cast(T.LongType()).alias("Unique_Items_Opt_Assorted"),
        F.lit(-1).cast(T.DoubleType()).alias("Cur_Width_X_Facing"),
        F.lit(-1).cast(T.DoubleType()).alias("Opt_Width_X_Facing"),
        F.lit(-1).cast(T.LongType()).alias("Cur_Opt_Same_Facing"),
        F.lit(-1).cast(T.DoubleType()).alias("Opt_Minus_Cur_Facings"),
        F.col("DEPARTMENT").alias("Department"),
        F.col("Cur_Margin").cast(T.FloatType()).alias("Opt_Margin"),
        F.col("Opt_Margin").cast(T.FloatType()).alias("Cur_Margin"),
        F.lit(True).alias("USES_CURRENT_FACINGS"),
    ]

    df_m = df_m.select(*cols_missing_slice)

    df_m = df_m.withColumn(
        "Cur_Width_X_Facing", F.col("Current_Facings") * F.col("Width")
    )
    df_m = df_m.withColumn(
        "Opt_Width_X_Facing", F.col("Optim_Facings") * F.col("Width")
    )

    # now set the 'USES_CURRENT_FACINGS' flag for non-missing slice
    df_o = df_o.withColumn("USES_CURRENT_FACINGS", F.lit(False))

    # some type conversion on the non-missing slice to ensure data unions
    cols_to_long = ["Unique_Items_Cur_Assorted", "Unique_Items_Opt_Assorted"]
    for col_name in cols_to_long:
        col = F.col(col_name).cast(T.LongType()).alias(col_name)
        df_o = df_o.withColumn(col_name, col)

    # combine both 'missing opt slice' and 'opt slice' together
    df = union_dfs([df_o, df_m], align_schemas=True)

    # final dup check to ensure there is no overlap between the two slices
    dup_check(df, ["Store_Physical_Location_No", "Item_No"])

    return df


def report_on_missing_facings(df: SparkDataFrame) -> SparkDataFrame:
    """
    reports on the missing store/SM combinations in terms of % missing

    Parameters
    ----------
    df : SparkDataFrame
        final optimization output with missing store/SM filled in with
        current facings, indicated by USES_CURRENT_FACINGS=True

    Returns
    -------
    None
    """

    col = F.when(F.col("USES_CURRENT_FACINGS") == True, F.lit(1)).otherwise(F.lit(0))
    agg = [F.sum(col).alias("N_MISSING"), F.count(F.col("*")).alias("N_TOTAL")]
    pdf_agg = df.groupBy("REGION", "BANNER").agg(*agg).toPandas()
    pdf_agg = pdf_agg.sort_values(["REGION", "BANNER"])
    pdf_agg["PER_MISSING"] = pdf_agg["N_MISSING"] / pdf_agg["N_TOTAL"]

    msg = f"""
    Percentage of Store/Section Master combinations which did not have 
    optimization output broken down by Region~Banner.
    For these records, the CURRENT facings were used:
    \n{pdf_agg}
    """

    log.info(msg)


def read_and_process_item_counts(
    df_bay_data: SparkDataFrame,
    item_output: SparkDataFrame,
    cluster_assignment: SparkDataFrame,
    region_list,
    banner_list,
):
    df_bay_data = df_bay_data.withColumnRenamed("MERGED_CLUSTER", "M_Cluster")
    df_bay_data = df_bay_data.withColumnRenamed("MERGE_CLUSTER", "M_Cluster")
    item_output = item_output.withColumnRenamed("MERGED_CLUSTER", "M_Cluster")
    item_output = item_output.withColumnRenamed("MERGE_CLUSTER", "M_Cluster")

    # get column names
    n = get_col_names()

    bay_data = df_bay_data.select(
        "NATIONAL_BANNER_DESC",
        "REGION",
        "SECTION_MASTER",
        "STORE_PHYSICAL_LOCATION_NO",
        n.F_ITEM_NO,
        "SALES",
        n.F_FACINGS,
        "E2E_MARGIN_TOTAL",
        "NEED_STATE",
        "ITEM_COUNT",
    )

    # in case we have stores with multiple facings over time represented in the df, we don't
    # want to duplciate the item counts so we drop dupes accross facings
    bay_data = bay_data.dropDuplicates(
        subset=["NEED_STATE", "STORE_PHYSICAL_LOCATION_NO", n.F_ITEM_NO]
    )
    bay_data = bay_data.withColumnRenamed(
        "National_Banner_Desc", n.F_BANNER
    ).withColumnRenamed("Region", n.F_REGION_DESC)

    # filter based on config region, banner, dept
    lower = F.lower(F.col(n.F_REGION_DESC))
    upper = F.upper(F.col(n.F_REGION_DESC))
    initcap = F.initcap(F.col(n.F_REGION_DESC))

    mask = lower.isin(*region_list) | (
        upper.isin(*region_list) | (initcap.isin(*region_list))
    )
    # filter POG on region and banner list
    bay_data = bay_data.filter(mask)

    bay_data = bay_data.filter(F.col(n.F_BANNER).isin(*banner_list))

    # drop any duplicates
    bay_data = bay_data.drop_duplicates(
        subset=[
            n.F_NEED_STATE,
            n.F_SECTION_MASTER,
            n.F_STORE_PHYS_NO,
            n.F_ITEM_NO,
            n.F_FACINGS,
        ]
    )

    # aggregate sales or margin across all facings now per store
    keys = [
        n.F_REGION_DESC,
        n.F_BANNER,
        n.F_SECTION_MASTER,
        n.F_NEED_STATE,
        n.F_STORE_PHYS_NO,
        n.F_ITEM_NO,
    ]
    # summary = bay_data.pivot_table(
    #     index=keys, values=n.F_ITEM_COUNT, aggfunc=np.sum
    # ).reset_index()
    summary = bay_data.groupBy(keys).agg(F.sum(n.F_ITEM_COUNT).alias(n.F_ITEM_COUNT))

    # merge store cluster in so we can aggregate on it
    # summary = summary.merge(cluster_assignment, on=[n.F_STORE_PHYS_NO], how="left")
    summary = summary.join(cluster_assignment, on=[n.F_STORE_PHYS_NO], how="left")

    # aggregate avg. sales across all stores now
    keys_avg = [
        n.F_REGION_DESC,
        n.F_BANNER,
        n.F_M_CLUSTER,
        n.F_SECTION_MASTER,
        n.F_NEED_STATE,
        n.F_ITEM_NO,
    ]
    # summary_avg = summary.pivot_table(
    #     index=keys_avg, values=n.F_ITEM_COUNT, aggfunc=np.mean
    # ).reset_index()
    summary_avg = summary.groupBy(keys_avg).agg(
        F.mean(n.F_ITEM_COUNT).alias(n.F_ITEM_COUNT)
    )

    # now we merge historic sales or margin into the item output
    # summary_avg.select(ITEM COUNT)
    # item_output_new = item_output.merge(summary_avg, on=keys_avg, how="left")
    summary_avg = summary_avg.withColumnRenamed(
        "Region_Desc", "REGION"
    ).withColumnRenamed("Banner", "BANNER")
    item_output_new = item_output.join(
        summary_avg,
        on=["REGION", "BANNER", "M_Cluster", "SECTION_MASTER", "NEED_STATE", "ITEM_NO"],
        how="left",
    )
    item_output_new = item_output_new.withColumnRenamed("M_Cluster", "MERGED_CLUSTER")

    return item_output_new


def get_margin_rerun_sales(
    df_opt_rerun: SparkDataFrame,
    df_elas_sales: SparkDataFrame,
    df_clusters: SparkDataFrame,
):
    """
    Get new projected sales of items with updated facings after margin rerun

    Parameters
    ----------
    df_opt_rerun : margin rerun outputs
    df_elas_sales : sales elasticity curves
    df_clusters : merged clusters lookup

    Returns
    -------
    opt_sales : SparkDataFrame
      margin rerun outputs with sales projections
    """
    df_opt_rerun = df_opt_rerun.join(df_clusters, on=["Store_Physical_Location_No"])
    df_opt_rerun = df_opt_rerun.withColumnRenamed("Region_Desc", "REGION")
    df_elas_sales = df_elas_sales.withColumnRenamed("National_Banner_Desc", "BANNER")

    opt_sales = df_opt_rerun.join(
        df_elas_sales,
        on=["REGION", "BANNER", "M_cluster", "SECTION_MASTER", "ITEM_NO"],
        how="left",
    )
    opt_sales = opt_sales.withColumn(
        "facing_fit_list", F.array(*[F.col("facing_fit_" + str(i)) for i in range(13)])
    )
    opt_sales = opt_sales.drop(*["facing_fit_" + str(i) for i in range(13)])
    opt_sales = opt_sales.withColumn(
        "Opt_Sales",
        opt_sales.facing_fit_list.getItem(F.col("Optim_Facings").cast(T.IntegerType())),
    )

    return opt_sales


def get_margin_rerun_margins(
    df_opt_rerun: SparkDataFrame,
    df_elas_margins: SparkDataFrame,
    df_clusters: SparkDataFrame,
):
    """
    Get projected margins of items in opt outputs

    Parameters
    ----------
    df_opt_rerun : opt outputs
    df_elas_margins : margin elasticity curves
    df_clusters : merged clusters lookup

    Returns
    -------
    opt_margins : SparkDataFrame
      opt outputs with margin projections
    """
    df_opt_rerun = df_opt_rerun.join(
        df_clusters, on=["Store_Physical_Location_No", "M_Cluster"]
    )
    df_opt_rerun = df_opt_rerun.withColumnRenamed("Region_Desc", "REGION")
    df_elas_margins = df_elas_margins.withColumnRenamed(
        "National_Banner_Desc", "BANNER"
    )
    opt_margins = df_opt_rerun.join(
        df_elas_margins.select(
            "REGION",
            "BANNER",
            "M_Cluster",
            "SECTION_MASTER",
            "ITEM_NO",
            *[F.col("facing_fit_" + str(i)) for i in range(13)],
        ),
        on=["REGION", "BANNER", "M_Cluster", "SECTION_MASTER", "ITEM_NO"],
        how="left",
    )

    opt_margins = opt_margins.withColumn(
        "facing_fit_list", F.array(*[F.col("facing_fit_" + str(i)) for i in range(13)])
    )
    opt_margins = opt_margins.drop(*["facing_fit_" + str(i) for i in range(13)])
    opt_margins = opt_margins.withColumn(
        "Opt_Margin",
        opt_margins.facing_fit_list.getItem(
            F.col("Optim_Facings").cast(T.IntegerType())
        ),
    )
    opt_margins = opt_margins.withColumn(
        "Cur_Margin",
        opt_margins.facing_fit_list.getItem(
            F.col("Current_Facings").cast(T.IntegerType())
        ),
    )

    # need to drop M_Cluster again
    opt_margins = opt_margins.drop("facing_fit_list")

    return opt_margins


def impute_null_sales_margin(df, df_bay_data, config_scope):
    """

    Parameters
    ----------
    df
    df_bay_data
    config_scope

    Returns
    -------

    """
    # Unify the dataframe column names
    df_bay_data = (
        df_bay_data.withColumnRenamed("NATIONAL_BANNER_DESC", "BANNER")
        .withColumnRenamed("E2E_MARGIN_TOTAL", "Margin")
        .withColumnRenamed("MERGED_CLUSTER", "M_CLUSTER")
    )

    # Transfer the Cur Sales and Cur Margin in df_bay_data
    txn_start_date = config_scope["st_date"]
    txn_end_date = config_scope["end_date"]
    start_date = pd.to_datetime(txn_start_date)
    end_date = pd.to_datetime(txn_end_date)
    no_of_weeks = (end_date - start_date) / np.timedelta64(1, "W")

    item_levels = [
        "REGION",
        "BANNER",
        "M_CLUSTER",
        "STORE_PHYSICAL_LOCATION_NO",
        "SECTION_MASTER",
        "NEED_STATE",
        "ITEM_NO",
    ]

    df_bay_data_filtered = (
        df_bay_data.select(item_levels + ["SALES", "MARGIN"])
        .groupby(item_levels)
        .agg(F.sum("SALES").alias("Sum_Sales"), F.sum("MARGIN").alias("Sum_Margin"))
        .withColumn("Real_Sales", F.col("Sum_Sales") / F.lit(no_of_weeks))
        .withColumn("Real_Margin", F.col("Sum_Margin") / F.lit(no_of_weeks))
        .select(item_levels + ["Real_Sales", "Real_Margin"])
    )
    # we have to do a left join here to ensure we don't drop newly assorted items in the original opt output
    df_result = (
        df.join(df_bay_data_filtered, on=item_levels, how="left")
        .withColumn("Cur_Sales", F.coalesce("Cur_Sales", "Real_Sales"))
        .withColumn("Opt_Sales", F.coalesce("Opt_Sales", "Cur_Sales"))
        .withColumn("Cur_Margin", F.coalesce("Cur_Margin", "Real_Margin"))
        .withColumn("Opt_Margin", F.coalesce("Opt_Margin", "Cur_Margin"))
    )

    return df_result
