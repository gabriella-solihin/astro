from typing import List, Tuple

import pandas as pd

from pyspark import Row
from pyspark.sql import DataFrame as SparkDataFrame
from spaceprod.src.utils import process_location, dedup_location
from spaceprod.utils.data_helpers import (
    df_to_pdf,
    strip_except_alpha_num,
    strip_except_alpha_num_in_pdf_columns,
)
from spaceprod.utils.data_transformation import union_dfs
from spaceprod.utils.imports import F
from spaceprod.utils.names import get_col_names
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.validation import dup_check


class RawDataContainer:
    """Class that contains all raw data needed for space opt"""

    def __init__(self):
        pass


def read_location_data(
    df_location: SparkDataFrame,
    regions: List[str],
    banners: List[str],
    global_exclusion_list: List[str],
    store_replace: List[str],
    store_replace_conf: Tuple[str, str],
):
    """this function reads the location data and converts it to pandas
    Parameters
    ----------
    spark: SparkSession
        spark session
    cfg: Dict
        optimization config

    Returns
    -------
    location: SparkDataFrame
        This dataframe has the store number mapping to physical number and address info
    """

    # get column names
    n = get_col_names()

    region_list = regions
    banner_list_copy = banners.copy()
    banner_list = [banner.upper() for banner in banner_list_copy]

    # dup checks on location before joining it in
    dims_location = ["STORE_NO", "STORE_PHYSICAL_LOCATION_NO"]
    df_location = process_location(df_location, dims_location)
    dup_check(df_location, dims_location)

    # filter to regions
    df_location = df_location.filter(F.lower(F.col("REGION_DESC")).isin(*region_list))

    if banner_list:
        mask = F.upper(F.col("NATIONAL_BANNER_DESC")).isin(*banner_list)
        df_location = df_location.filter(mask)

    # get only required columns
    df_location = df_location.selectExpr(
        n.F_RETAIL_LOC_SK,
        n.F_STORE_PHYS_NO,
        "NATIONAL_BANNER_DESC as Banner",
        n.F_STORE_NO,
        n.F_REGION_DESC,
        "ACTIVE_STATUS_CD",
    )

    dup_check(df_location, [n.F_RETAIL_LOC_SK])

    # add a new 'location' record
    # TODO: explain why we need this
    new_sk = "26978"

    df_record_to_add = spark.createDataFrame(
        [
            Row(
                Retail_Outlet_Location_Sk=new_sk,
                Store_Physical_Location_No="11429",
                Banner="SOBEYS",
                Store_No="4761",
                Region_Desc="Ontario",
            )
        ]
    )

    # make sure to exclude the existing SK (if it exists) so that we don't
    # create dups
    mask = F.col("Retail_Outlet_Location_Sk") != new_sk

    # combine the 2 slices together
    df_location = union_dfs(
        dfs=[df_location.filter(mask), df_record_to_add],
        align_schemas=True,
    )

    # do a final dup check to make sure we did not introduce duplicates on SK
    # do NOT disable this because this can cause disaster downstream
    dup_check(
        df=df_location,
        dims=["Retail_Outlet_Location_Sk"],
        assert_=True,
        msg="The new 'location' record that was added introduces dups. Check the new record",
    )

    df_location = filter_out_exlusion_store_no_from_trx(
        df_location=df_location,
        global_exclusion_list=global_exclusion_list,
        store_replace=store_replace,
        store_replace_conf=store_replace_conf,
    )

    # follow universal convention
    df_location = df_location.withColumn(
        n.F_REGION_DESC, F.lower(F.col(n.F_REGION_DESC))
    )
    df_location = df_location.withColumn(n.F_BANNER, F.upper(F.col(n.F_BANNER)))

    # convert to pandas for compatiability
    pdf_location = df_location.toPandas()

    ren = {"STORE_NO": "Store_No"}
    pdf_location = pdf_location.rename(columns=ren, errors="raise")

    return pdf_location, df_location


def read_product_df(
    df_product: SparkDataFrame,
):
    """this function reads the product data and converts it to pandas
    Parameters
    ----------
    spark: SparkSession
        spark session
    cfg: Dict
        optimization config
    n: dict
        this is the names dict that includes all column names and rename lists / dictionaries

    Returns
    -------
    product_df: pd.DataFrame
        This dataframe has the product information like lvl 5 category information and category id
    """
    """this function reads the product data and converts it to pandas
    Parameters
    ----------
    spark: SparkSession
        spark session
    cfg: Dict
        optimization config
    n: dict
        this is the names dict that includes all column names and rename lists / dictionaries

    Returns
    -------
    product_df: pd.DataFrame
        This dataframe has the product information like lvl 5 category information and category id
    """

    # get column names
    n = get_col_names()

    product_df = df_product.select(
        n.F_LVL5_NAME,
        n.F_LVL4_NAME,
        n.F_LVL3_NAME,
        n.F_LVL2_NAME,
        n.F_ITEM_NAME,
        n.F_ITEM_NO,
    )

    product_df = product_df.dropDuplicates(subset=[n.F_ITEM_NO]).toPandas()
    return product_df


def read_bay_data_pre_index(
    df_bay_data: SparkDataFrame,
    pdf_pog_department_mapping: pd.DataFrame,
    regions: List[str],
    banners: List[str],
    depts: List[str],
) -> pd.DataFrame:
    """This function reads the historic sales or margin to model and filters on the subset of region banners

    Parameters
    ----------
    spark: sparkSession
    cfg: dict
        optimization config
    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries

    Returns
    -------
    bay_data: pd.DataFrame
        This df has the historic sales and margin per facing as well as units sold
    """
    # get column names
    n = get_col_names()

    region_list = regions
    banner_list_copy = banners.copy()
    banner_list = [banner.upper() for banner in banner_list_copy]

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

    # filter department
    # merge in department onto section master to filter on it
    POG_department_mapping = spark.createDataFrame(pdf_pog_department_mapping)
    POG_department_mapping = POG_department_mapping.select(
        n.F_REGION_DESC, n.F_BANNER, n.F_SECTION_MASTER, n.F_DEPARTMENT
    )

    POG_department_mapping = POG_department_mapping.filter(
        F.col(n.F_DEPARTMENT).isin(*depts)
    )

    bay_data = bay_data.join(
        POG_department_mapping,
        on=[n.F_REGION_DESC, n.F_BANNER, n.F_SECTION_MASTER],
        how="inner",
    )
    pdf = bay_data.toPandas()

    pdf = strip_except_alpha_num_in_pdf_columns(pdf)

    # drop any duplicates
    pdf = pdf.drop_duplicates(
        subset=[
            n.F_NEED_STATE,
            n.F_SECTION_MASTER,
            n.F_STORE_PHYS_NO,
            n.F_ITEM_NO,
            n.F_FACINGS,
        ]
    )
    return pdf


def read_manual_input_file(
    df: SparkDataFrame,
    change_region_banner_convention=False,
) -> pd.DataFrame:
    """this function reads manual constraint files like supplier to item mapping file and converts to pandas
    Parameters
    ----------
    spark: SparkSession
        spark session
    path: str
        path on the blob to read from
    change_region_banner_convention: bool
        if true we make sure region banner columns are in right upper or lower case convention

    Returns
    -------
    df: pd.DataFrame
        This df contains manual input file for constraints like the mapping between supplier IDs and Item NO
    """

    # get column names
    n = get_col_names()

    if change_region_banner_convention:
        # follow universal convention
        df = df.withColumn(n.F_REGION_DESC, F.lower(F.col(n.F_REGION_DESC)))
        df = df.withColumn(n.F_BANNER, F.upper(F.col(n.F_BANNER)))

    pdf = df.toPandas()

    pdf = strip_except_alpha_num_in_pdf_columns(pdf)

    return pdf


def read_and_filter_local_default_space(
    df: SparkDataFrame,
    location: SparkDataFrame,
    regions: List[str],
    banners: List[str],
) -> pd.DataFrame:

    # get column names
    n = get_col_names()

    region_list = regions
    banner_list_copy = banners.copy()
    banner_list = [banner.upper() for banner in banner_list_copy]

    df = df.withColumnRenamed("National_Banner_Desc", n.F_BANNER)

    # add in store physical from location
    location = location.select(n.F_BANNER, n.F_STORE_NO, n.F_STORE_PHYS_NO)

    # merge in location data so that we know region banner
    df = df.join(location, on=[n.F_BANNER, n.F_STORE_NO], how="inner")

    # filter based on config region, banner, dept
    lower = F.lower(F.col(n.F_REGION_DESC))
    upper = F.upper(F.col(n.F_REGION_DESC))
    initcap = F.initcap(F.col(n.F_REGION_DESC))

    mask = lower.isin(*region_list) | (
        upper.isin(*region_list) | (initcap.isin(*region_list))
    )
    # filter POG on region and banner list
    df = df.filter(mask)

    df = df.filter(F.col(n.F_BANNER).isin(*banner_list))

    # change region banner convention
    df = df.withColumn(n.F_REGION_DESC, F.lower(F.col(n.F_REGION_DESC)))
    df = df.withColumn(n.F_BANNER, F.upper(F.col(n.F_BANNER)))

    pdf = df.toPandas()

    pdf = strip_except_alpha_num_in_pdf_columns(pdf)

    return pdf


def read_merged_clusters_df(
    df_merged_clusters: SparkDataFrame,
    df_merged_clusters_external: SparkDataFrame,
    pdf_location: pd.DataFrame,
    use_merged_clusters_post_review: bool,
):
    """this function reads the merged clusters file
    Parameters
    ----------
    Returns
    -------
    merged_clusters_df: pd.DataFrame
        This df has the internal and external store cluster merged assignment and also
        region and banner columns for filtering
    """

    # get column names
    n = get_col_names()

    pdf_location = pdf_location[[n.F_STORE_PHYS_NO, n.F_STORE_NO]]

    if use_merged_clusters_post_review:
        pdf_merged_clusters = df_merged_clusters_external.toPandas()
    else:
        # merge in store no because it's missing from the original merged cluster concatenation file
        df_location = spark.createDataFrame(pdf_location)
        df_merged_clusters = df_merged_clusters.join(
            df_location, on=[n.F_STORE_PHYS_NO], how="inner"
        )

        pdf_merged_clusters = df_merged_clusters.toPandas()

    pdf_merged_clusters.rename(
        columns=dict(
            zip(
                pdf_merged_clusters.columns,
                [strip_except_alpha_num(x) for x in pdf_merged_clusters.columns],
            )
        ),
        inplace=True,
    )
    # folllow convention
    pdf_merged_clusters[n.F_BANNER] = pdf_merged_clusters[n.F_BANNER].str.upper()
    pdf_merged_clusters = pdf_merged_clusters.rename(
        {"Region": n.F_REGION_DESC}, axis=1
    )
    pdf_merged_clusters[n.F_REGION_DESC] = pdf_merged_clusters[
        n.F_REGION_DESC
    ].str.lower()

    return pdf_merged_clusters


def read_shelve_space_and_department_mapping_data(
    pdf_location: pd.DataFrame,
    df_pog_input_file: SparkDataFrame,
    df_pog_department_mapping_file: SparkDataFrame,
    regions: List[str],
    banners: List[str],
    depts: List[str],
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """this function reads the plannogram data (aka shelve space data), filters on the applicable region and banner list and converts it to pandas
    Subsequently, we add in department to filter on to the POG
    Parameters
    ----------
    spark: SparkSession
        spark session
    cfg: Dict
        optimization config
    n: dict
        this is the names dict that includes all column names and rename lists / dictionaries
    location: sparkDataFrame
        We merge in location to get region and banner into this because we want to filter on only relevant POGs

    Returns
    -------
    shelve_space_df: pd.DataFrame
        This has the item width per item and facings in the file.
    POG_department_mapping: pd.DataFrame
        this is used in the elasticity curves to also map on dept and filter on dept.
    """

    # get column names
    n = get_col_names()

    region_list = regions
    banner_list = banners
    dept_list = depts

    df_location = spark.createDataFrame(pdf_location)

    df_pog_input_file = df_pog_input_file.withColumnRenamed("STORE", "STORE_NO")
    df_location = df_location.select(n.F_BANNER, "STORE_NO", "Region_Desc")

    # filter out POG facings with null
    df_pog_input_file = df_pog_input_file.where(~F.col("FACINGS").isNull())

    # merge in location data so that we know region banner
    df_pog_input_file = df_pog_input_file.join(
        df_location, on=["STORE_NO"], how="inner"
    )

    # filter based on config region, banner, dept
    lower = F.lower(F.col("Region_Desc"))

    mask = lower.isin(*[x.lower() for x in region_list])
    # filter POG on region and banner list
    df_pog_input_file = df_pog_input_file.filter(mask)

    if banner_list:
        mask = F.col("Banner").isin(*banner_list)
        df_pog_input_file = df_pog_input_file.filter(mask)

    pdf_pog_input_file = df_to_pdf(df_pog_input_file)

    pdf_pog_input_file.rename(n.F_SHELVE_SPACE_DF_NEW_COL_NAMES, axis=1, inplace=True)

    # we now read in the dept. mapping so that we can filter on the department we are interested in
    pdf_pog_department_mapping_file = df_pog_department_mapping_file.toPandas()

    # filter for only in scope store sections
    pdf_pog_department_mapping_file = pdf_pog_department_mapping_file.loc[
        pdf_pog_department_mapping_file["In_Scope"] == "1"
    ]
    assert (
        len(pdf_pog_department_mapping_file) > 0
    ), "POG is empty after in scope filtering"

    pdf_pog_department_mapping_file.rename(
        n.F_POG_DEPT_MAPPING_DF_NEW_COL_NAMES, axis=1, inplace=True
    )

    # follow region banner convention
    pdf_pog_department_mapping_file = pdf_pog_department_mapping_file.rename(
        columns={"Region": n.F_REGION_DESC},
        errors="raise",
    )

    pdf_pog_department_mapping_file[n.F_REGION_DESC] = pdf_pog_department_mapping_file[
        n.F_REGION_DESC
    ].str.lower()

    pdf_pog_department_mapping_file[n.F_BANNER] = pdf_pog_department_mapping_file[
        n.F_BANNER
    ].str.upper()

    # merge and filter on specific region banner dept to have correct mapping
    pdf_pog_input_file = pdf_pog_input_file.merge(
        pdf_pog_department_mapping_file[
            [n.F_SECTION_MASTER, n.F_REGION_DESC, n.F_BANNER, n.F_DEPARTMENT]
        ],
        on=[n.F_SECTION_MASTER, n.F_REGION_DESC, n.F_BANNER],
        how="inner",
    )

    if dept_list:
        # Filter on departments in config scope
        pdf_pog_input_file = pdf_pog_input_file.loc[
            pdf_pog_input_file[n.F_DEPARTMENT].isin(dept_list)
        ]

    return pdf_pog_input_file, pdf_pog_department_mapping_file


def read_elasticity_curves(
    df_elasticity_curves_sales: SparkDataFrame,
    df_elasticity_curves_margin: SparkDataFrame,
    regions: List[str],
    banners: List[str],
    depts: List[str],
    POG_department_mapping: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """This function reads the spark parquet of elasticity fits and return pandas-converted df
    Depending on the path in the config, it reads either micro or macro curves

    Parameters
    ----------
    spark: SparkSession
        spark session
    cfg: Dict
        optimization config
    n: dict
        this is the names dict that includes all column names and rename lists / dictionaries
    POG_department_mapping: pd.DataFrame
        used to filter on department

    Returns
    -------
    elasticity_curves_sales: pd.DataFrame
        Dataframe that has the sales curves (facings costs)
    elasticity_curves_margin: pd.DataFrame
        Dataframe that has the margin curves (facings costs)
    """

    # get column names
    n = get_col_names()

    # rename for filtering
    df_elasticity_curves_sales = df_elasticity_curves_sales.withColumnRenamed(
        "REGION", "REGION_DESC"
    ).withColumnRenamed("NATIONAL_BANNER_DESC", "BANNER")

    # merge in department onto section master to filter on it
    POG_department_mapping = spark.createDataFrame(POG_department_mapping)
    POG_department_mapping = POG_department_mapping.select(
        n.F_REGION_DESC, n.F_BANNER, n.F_SECTION_MASTER, n.F_DEPARTMENT
    )
    df_elasticity_curves_sales = df_elasticity_curves_sales.join(
        POG_department_mapping,
        on=[n.F_REGION_DESC, n.F_BANNER, n.F_SECTION_MASTER],
        how="inner",
    )

    # filter on region banner departments in scope of optimization
    df_elasticity_curves_sales = filter_region_banner_department(
        df=df_elasticity_curves_sales,
        regions=regions,
        banners=banners,
        depts=depts,
    )

    df_elasticity_curves_sales = df_elasticity_curves_sales.toPandas()

    df_elasticity_curves_sales.rename(
        columns=dict(
            zip(
                df_elasticity_curves_sales.columns,
                [strip_except_alpha_num(x) for x in df_elasticity_curves_sales.columns],
            )
        ),
        inplace=True,
    )
    df_elasticity_curves_sales = df_elasticity_curves_sales.rename(
        n.F_ELASTICITY_CURVE_NEW_NAMES, axis=1
    )

    # rename for filtering
    df_elasticity_curves_margin = df_elasticity_curves_margin.withColumnRenamed(
        "REGION", "REGION_DESC"
    ).withColumnRenamed("NATIONAL_BANNER_DESC", "BANNER")

    # merge in department onto section master to filter on it
    df_elasticity_curves_margin = df_elasticity_curves_margin.join(
        POG_department_mapping,
        on=[n.F_REGION_DESC, n.F_BANNER, n.F_SECTION_MASTER],
        how="inner",
    )

    # filter on region banner departments in scope of optimization
    df_elasticity_curves_margin = filter_region_banner_department(
        df=df_elasticity_curves_margin,
        regions=regions,
        banners=banners,
        depts=depts,
    )

    df_elasticity_curves_margin = df_elasticity_curves_margin.toPandas()

    df_elasticity_curves_margin.rename(
        columns=dict(
            zip(
                df_elasticity_curves_margin.columns,
                [
                    strip_except_alpha_num(x)
                    for x in df_elasticity_curves_margin.columns
                ],
            )
        ),
        inplace=True,
    )

    df_elasticity_curves_margin = df_elasticity_curves_margin.rename(
        n.F_ELASTICITY_CURVE_NEW_NAMES, axis=1
    )

    df_elasticity_curves_sales[n.F_SALES] = df_elasticity_curves_sales[
        n.F_SALES
    ].astype(float)
    df_elasticity_curves_margin[n.F_MARGIN] = df_elasticity_curves_margin[
        n.F_MARGIN
    ].astype(float)
    return df_elasticity_curves_sales, df_elasticity_curves_margin


def filter_region_banner_department(
    df: SparkDataFrame,
    regions: List[str],
    banners: List[str],
    depts: List[str],
):
    """This function reads the region banner dept. in scope from config and filter df on it

    Parameters
    ----------
    cfg: Dict
        optimization config
    df: SparkDataFrame
        df to be filtered on

    Returns
    -------
    df: SparkDataFrame
        filtered df
    """

    mask = F.lower(F.col("Region_Desc")).isin([x.lower() for x in regions])
    df = df.filter(mask)

    if banners:
        df = df.filter(F.col("Banner").isin(*banners))

    if depts:
        df = df.filter(F.col("Department").isin(*depts))

    return df


def filter_out_exlusion_store_no_from_trx(
    df_location: SparkDataFrame,
    global_exclusion_list: List[str],
    store_replace: List[str],
    store_replace_conf: Tuple[str, str],
):
    """Function takes spark dataframe and filters out stores

    Parameters
    ----------
    df_location : SparkDataFrame
        transactions raw data

    Returns
    -------
    trx_raw : SparkDataFrame
        this is the transactions data frame excluding stores we want to exclude
    """

    df = df_location

    # filter stores to subset of stores of interest
    df = df.filter(~F.col("STORE_NO").isin(global_exclusion_list))

    df = df.filter(~F.col("STORE_PHYSICAL_LOCATION_NO").isin(store_replace))

    # also overwrite one store NO. because of data issue
    df = df.withColumn(
        "STORE_NO",
        F.when(
            F.col("STORE_NO") == store_replace_conf[0],
            store_replace_conf[1],
        ).otherwise(df["STORE_NO"]),
    )

    return df


def determine_local_items(
    df: SparkDataFrame,
    df_location: SparkDataFrame,
    min_sold_count: int,
    ratio_threshold: int,
) -> SparkDataFrame:
    """
    Determines the list of 'local item' which is defined as these
    that are sold in only one single store at the moment.

    Parameters
    ----------
    df:
        a sparkDataFrame that contains the sale info at the level of
        region, banner, store and item.

    min_sold_count:
        specify the minimum basket count during the time frame
        specified by st_date and end_date. This threshold value is
        used to filter out items with too few sold baskets.

    ratio_threshold:
        specify the basket ratio.

    Returns
    -------
    df_localized_items_default:
        return the dataframe at the granularity of region, banner,
        store and items level. these are considered to be localized
        items.
    """

    # granularity of aggregation
    dims = ["REGION", "NATIONAL_BANNER_DESC", "ITEM_NO"]
    ttl_qty = F.sum("Item_Count")

    # agg1
    region_banner_item_agg = df.groupby(dims).agg(
        ttl_qty.alias("region_banner_item_ttl_qty")
    )
    # agg2
    region_banner_store_item_store_agg = df.groupby(
        dims + ["STORE_PHYSICAL_LOCATION_NO"]
    ).agg(ttl_qty.alias("region_banner_store_item_ttl_qty"))

    # ratio
    ratio = F.col("region_banner_store_item_ttl_qty") / F.col(
        "region_banner_item_ttl_qty"
    )

    ratio_df = (
        region_banner_store_item_store_agg.join(
            region_banner_item_agg, on=dims, how="inner"
        )
        .withColumn("ttl_qty_RATIO", F.round(ratio, 3))
        .select(
            "REGION",
            "NATIONAL_BANNER_DESC",
            "STORE_PHYSICAL_LOCATION_NO",
            "ITEM_NO",
            "region_banner_store_item_ttl_qty",
            "region_banner_item_ttl_qty",
            "ttl_qty_RATIO",
        )
    )

    # define local items
    cond1 = F.col("ttl_qty_RATIO") > ratio_threshold
    cond2 = F.col("region_banner_store_item_ttl_qty") > min_sold_count
    mask = cond1 & cond2
    ratio_df_filtered = ratio_df.filter(mask)

    # dup checks on location before joining it in
    dims_location = ["STORE_NO", "STORE_PHYSICAL_LOCATION_NO"]
    df_location = process_location(df_location, dims_location)
    dup_check(df_location, dims_location)
    df_location = df_location.select(*dims_location)

    # to get store_no for downstream join(s)
    df_localized_items_default = ratio_df_filtered.join(
        other=df_location, on="STORE_PHYSICAL_LOCATION_NO", how="inner"
    )

    return df_localized_items_default


def determine_space_pct_for_local_items(
    local_items: SparkDataFrame,
    combined_pog: SparkDataFrame,
    df_location: SparkDataFrame,
) -> SparkDataFrame:
    """
    This function takes the local item flag generated upstream, and
    append the flag to POG file. Then at the level of region, banner,
    store and category level, sums up the space for local items and
    non local items. The space here is defined by item_width time
    the number of facings in the POG file.

    Parameters
    ----------
    local_items:
        a sparkDataFrame that contains local items.

    combined_pog:
        the processed combined POG file.

    df_location:
        the processed location input.

    Returns
    -------
    df_localized_items_default:
        return the space for local and non-local items at the
        granularity of region banner store and category.
    """

    # dup checks on location before joining it in
    dims_location = ["STORE_NO", "STORE_PHYSICAL_LOCATION_NO"]
    df_location = process_location(df_location, dims_location)
    dup_check(df_location, dims_location)
    cols = ["STORE_NO", "REGION_DESC", F.col("Banner").alias("NATIONAL_BANNER_DESC")]
    df_location = df_location.select(*cols)

    # POG
    combined_pog = combined_pog.withColumnRenamed("STORE", "STORE_NO")
    combined_pog = combined_pog.filter(F.col("item_no").isNotNull())
    combined_pog = combined_pog.join(df_location, on=["STORE_NO"], how="inner")

    dims = [
        "REGION_DESC",
        "NATIONAL_BANNER_DESC",
        "STORE_NO",
        "SECTION_MASTER",
    ]

    # POG + local item(region, banner, store, local items)
    cols = dims + ["ITEM_NO", "ttl_qty_RATIO", "FACINGS", "WIDTH"]
    col_local_space = F.when(
        F.col("ttl_qty_RATIO").isNotNull(), F.col("SPACE")
    ).otherwise(0)
    base = combined_pog.join(
        local_items.select("ITEM_NO", "STORE_NO", "ttl_qty_RATIO").dropDuplicates(),
        on=["ITEM_NO", "STORE_NO"],
        how="left",
    )

    df_space = (
        base.select(*cols)
        .withColumn("SPACE", F.col("FACINGS") * F.col("WIDTH"))
        .withColumn("LOCAL_SPACE", col_local_space)
    )

    # absolute & pct
    total_space = F.sum("SPACE").alias("TOTAL_SPACE")
    local_total_space = F.sum("LOCAL_SPACE").alias("LOCAL_TOTAL_SPACE")
    local_space_pct = F.col("LOCAL_TOTAL_SPACE") / F.col("TOTAL_SPACE")

    # aggregation
    df_localized_default_space = (
        df_space.groupBy(dims)
        .agg(total_space, local_total_space)
        .withColumn("LOCAL_SPACE_PCT", local_space_pct)
    )

    ren = ["NATIONAL_BANNER_DESC", "Banner"]
    df_localized_default_space = df_localized_default_space.withColumnRenamed(*ren)

    return df_localized_default_space
