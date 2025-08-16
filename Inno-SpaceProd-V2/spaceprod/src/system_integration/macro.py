from typing import Dict, List

import numpy as np
import pandas as pd
from pyspark.sql.types import IntegerType

from spaceprod.src.system_integration import data_contracts as dc
from spaceprod.src.utils import process_location
from spaceprod.utils.data_helpers import backup_on_blob
from spaceprod.utils.data_transformation import (
    apply_data_definition,
    apply_data_definition_pd,
    is_col_null_mask,
    union_dfs,
)
from spaceprod.utils.imports import F, SparkDataFrame, T
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.validation import dup_check, dup_check_pd


def create_elasticity_lookup(
    df_final_elasticity_output: SparkDataFrame,
    df_merge_clusters: SparkDataFrame,
) -> SparkDataFrame:
    """
    Creates an elasticity lookup matrix, containing information on elasticity
    store/section master level.
    The elasticity information is represented as a list of maps between
    item number and a list of facings.

    Parameters
    ----------
    df_final_elasticity_output : SparkDataFrame
        outut of the elasticity module (either sales or margin)

    df_merge_clusters : SparkDataFrame
        output of the clustering module that contains merged cluster
        information by store

    Returns
    -------
    elasticity lookup matrix on store/section master level
    """

    # shortcuts
    df = df_final_elasticity_output
    df_mc = df_merge_clusters

    # quick dup check
    dims = ["Region", "National_Banner_Desc", "Section_Master", "Item_No", "M_Cluster"]
    dup_check(df, dims)

    dims = ["REGION", "BANNER", "STORE_PHYSICAL_LOCATION_NO"]
    dup_check(df_mc, dims)

    # create 2 lists of columns that specify facing fits as well
    # as all other columns
    cols = df.columns
    facing_column_starts_with = "facing_fit_"
    cols_fac = [x for x in cols if x.startswith(facing_column_starts_with)]
    list_fac = [int(x[len(facing_column_starts_with) :]) for x in cols_fac]
    cols_fac_ordered = [f"{facing_column_starts_with}{x}" for x in sorted(list_fac)]

    # create the FACINGS_LIST column a list of
    # we round to nearest 1-decimal to reduce the size of ata
    list_cols = [F.round(F.col(x), 1) for x in cols_fac_ordered]
    col_facings_list = F.array(*list_cols)
    df = df.withColumn("FACINGS_LIST", col_facings_list)

    # exclude the first element if it is 0.0
    col_fac = F.col("FACINGS_LIST")
    col_fac_first = col_fac.getItem(0)
    col_facing_rest = F.slice(col_fac, start=2, length=999999)
    col = F.when(col_fac_first == 0.0, col_facing_rest).otherwise(col_fac)
    df = df.withColumn("FACINGS_LIST", col)

    # perform aggregation by core dimensions + NS
    # this groups by the core dimensions (reg + banner + SM + M Cluster) + the
    # NS dimension, which will also be aggregated away in the next aggregation.
    # we perform this aggregation in 2 steps like this because the resulting
    # elasticity column is a nested mapping like this:
    # NS -> Item No -> Facings List
    col_facings_map = F.create_map(F.col("Item_No"), F.col("FACINGS_LIST"))
    agg = [F.collect_list(col_facings_map).alias("FACINGS_MAP")]
    dims_core = ["Region", "National_Banner_Desc", "Section_Master", "M_Cluster"]
    dims = dims_core + ["Need_State"]
    df_agg = df.groupBy(*dims).agg(*agg)

    # perform final aggregation by core dimensions
    col_facings_map = F.create_map(F.col("Need_State"), F.col("FACINGS_MAP"))
    agg = [F.collect_list(col_facings_map).alias("FACINGS_MAP")]
    dims = ["Region", "National_Banner_Desc", "Section_Master", "M_Cluster"]
    df_agg = df_agg.groupBy(*dims).agg(*agg)

    # join together the STORE NO information with the FACINGS information
    # NOTE: this will explode because we are bringing in the SECTION_MASTER
    # which is expected

    rename_map = {
        "Region": "REGION",
        "National_Banner_Desc": "BANNER",
        "M_Cluster": "MERGED_CLUSTER",
        "Section_Master": "SECTION_MASTER",
        "FACINGS_MAP": "FACINGS_MAP",
    }

    # we want a dataset that contains both:
    # SECTION_MASTER and STORE_PHYSICAL_LOCATION_NO
    # therefore this join EXPLODES on
    # SECTION_MASTER and STORE_PHYSICAL_LOCATION_NO
    dims = ["REGION", "BANNER", "MERGED_CLUSTER"]
    cols = [F.col(name).alias(label) for name, label in rename_map.items()]
    df_facings_lookup = df_mc.join(df_agg.select(*cols), dims, "left")

    # if explosion is correct, should be unique on these dims
    dims = ["REGION", "BANNER", "STORE_PHYSICAL_LOCATION_NO", "SECTION_MASTER"]
    dup_check(df_facings_lookup, dims)

    # now we should be able to remove the region/banner info as it should
    # be incorporated into the STORE_PHYSICAL_LOCATION_NO
    dims = ["STORE_PHYSICAL_LOCATION_NO", "SECTION_MASTER"]
    dup_check(df_facings_lookup, dims)

    return df_facings_lookup


def combine_elasticity_lookup(
    df_elasticity_lu_sales: SparkDataFrame,
    df_elasticity_lu_margin: SparkDataFrame,
) -> SparkDataFrame:
    """
    Combines the elasticity lookup matrices between 'sales' and 'margin'
    by unioning them together and adding a new dimensional column
    'sales_or_margin'

    Parameters
    ----------
    df_elasticity_lu_sales : SparkDataFrame
        elasticity lookup matrix for 'sales'

    df_elasticity_lu_margin : SparkDataFrame
        elasticity lookup matrix for 'margin'

    Returns
    -------
    combined elasticity lookup matrix for 'sales' and 'margin' in one dataset
    """

    df_elasticity_lu_sales = df_elasticity_lu_sales.withColumn(
        "sales_or_margin", F.lit("sales")
    )
    df_elasticity_lu_margin = df_elasticity_lu_margin.withColumn(
        "sales_or_margin", F.lit("margin")
    )

    df_elasticity_lu = union_dfs(
        dfs=[df_elasticity_lu_sales, df_elasticity_lu_margin],
        align_schemas=True,
    )

    df_elasticity_lu = backup_on_blob(spark, df_elasticity_lu)

    dims = ["STORE_PHYSICAL_LOCATION_NO", "SECTION_MASTER", "sales_or_margin"]
    dup_check(df_elasticity_lu, dims)
    return df_elasticity_lu


def create_macro_skeleton(
    df_store_cluster_mapping: SparkDataFrame,
    df_macro_output_raw: SparkDataFrame,
) -> pd.DataFrame:
    """
    Creates the skeleton for the macro output,
     i.e. only the dimensional data + some attributes

    Parameters
    ----------
    df_store_cluster_mapping : output of the upstream system file task
    df_macro_output_raw :
    df_summary_legal_breaks :


    Returns
    -------
    skeleton dataset for the macro system file
    """

    # convert inputs to pandas for more efficient processing as the data is
    # is not a granular micro data
    pdf_store_cluster_mapping = df_store_cluster_mapping.toPandas()
    pdf_macro_output_raw = df_macro_output_raw.toPandas()

    # quick dup checks to make sure inputs are valid
    dims = ["store_physical_location_id", "item_no"]
    dup_check_pd(pdf_macro_output_raw, dims)

    # aggregate away the item
    dims = ["store_physical_location_id", "section_master"]

    agg = {
        "opt_sales": pd.NamedAgg("opt_sales", "sum"),
        "cur_sales": pd.NamedAgg("cur_sales", "sum"),
        "current_facings": pd.NamedAgg("current_facings", "sum"),
    }

    pdf = pdf_macro_output_raw.groupby(dims).agg(**agg).reset_index()

    # the following columns should only be on store/SM level
    # use them to create the lookup and ensure we only have 1 value
    # per store/SM
    cols = [
        "store_physical_location_id",
        "section_master",
        "cur_legal_breaks",
        "opt_legal_breaks",
        "legal_increment_width",
    ]

    pdf_store_sm_lookup = pdf_macro_output_raw[cols].drop_duplicates()
    dims = ["store_physical_location_id", "section_master"]
    dup_check_pd(pdf_store_sm_lookup, dims)

    # add in the cur and optimal legal breaks
    pdf = pdf.merge(
        right=pdf_store_sm_lookup,
        on=["store_physical_location_id", "section_master"],
        how="left",
    )

    # check that the join is clean
    check = all([not pdf[x].isnull().any() for x in cols])
    assert check, f"Some of the columns are invalid: {cols}"

    dims = ["store_physical_location_id", "section_master"]
    dup_check_pd(pdf, dims)

    # if we ever have store scoping inside store_cluster_mapping
    # this step will filter only to the scope of stores in store_cluster_mapping
    list_stores = pdf_store_cluster_mapping["store_physical_location_id"].unique()
    pilot_filter = pdf["store_physical_location_id"].isin(list_stores)
    pdf = pdf[pilot_filter]

    return pdf


def add_col_store_no_macro(
    pdf: pd.DataFrame, df_location: SparkDataFrame
) -> pd.DataFrame:
    """
    Adds 'store_no' column to the macro output
    Parameters
    ----------
    pdf : pd.DataFrame
        macro dataset

    df_location : SparkDataFrame
        external location data

    Returns
    -------
    macro dataset with 'store_no'
    """

    # shortcut
    df_loc = df_location

    # dedup to make sure we
    cols = ["STORE_NO", "STORE_PHYSICAL_LOCATION_NO"]
    df_loc = process_location(df_loc, "STORE_PHYSICAL_LOCATION_NO").select(*cols)

    # TODO: there was a weird behaviour in spark where the .toPandas()
    #  conversion on the next line was replacing values in the data and making
    #  all rows in data the same. it was not possible to reproduce this over
    #  databricks-connect and only occured on DevOps Pipelines, the below
    #  backup_on_blob seems to be a workaround for this
    df_loc = backup_on_blob(spark, df_loc)

    pdf_loc = df_loc.toPandas()

    # rename the column name for compatibility
    ren = {
        "STORE_PHYSICAL_LOCATION_NO": "store_physical_location_id",
        "STORE_NO": "store_no",
    }

    pdf_loc = pdf_loc.rename(columns=ren, errors="raise")

    # do a dup check at the beginning and the end on the main data to ensure
    # that the join did not cause explosion. I.e. an explosion can happen
    # if we ever have more than 1 store_no for each store_physical_location_id
    dup_check_pd(pdf, ["store_physical_location_id", "section_master"])

    pdf_loc = pdf_loc.drop_duplicates(subset=["store_physical_location_id"])

    # perform the join to lookup 'store_no'
    pdf = pdf.merge(right=pdf_loc, on="store_physical_location_id", how="left")

    # do post-merge dup check to make sure no explosion happened
    # (see comment above)
    dup_check_pd(pdf, ["store_physical_location_id", "section_master"])

    # ensure the the join is clean (no non-matches)
    msg = "bad store_no lookup for macro"
    assert not pdf["store_no"].isnull().any(), msg

    return pdf


def add_col_segmentation_cluster_and_size_uom(
    pdf: pd.DataFrame,
    df_store_cluster_mapping: SparkDataFrame,
) -> pd.DataFrame:
    """
    Adds store information to the macro data on `store_physical_location_id`
    level: segmentation_cluster and size_uom
    Parameters
    ----------
    pdf : pd.DataFrame
        macro system data output

    df_store_cluster_mapping :
        output of the upstream system integration task

    Returns
    -------
    macro output with segmentation_cluster and size_uom columns
    """

    pdf_scm = df_store_cluster_mapping.toPandas()

    dims = ["store_physical_location_id", "store_cluster_id"]
    dup_check_pd(pdf_scm, dims)

    pdf_scm.rename({"category": "section_master"}, axis=1, inplace=True)
    store_info_cols = [
        "store_physical_location_id",
        "segmentation_cluster",
        "size_uom",
        "section_master",
    ]

    pdf = pdf.merge(
        right=pdf_scm[store_info_cols],
        on=["store_physical_location_id", "section_master"],
        how="inner",
    )

    return pdf


def add_col_logical_segment_size_ft(
    pdf: pd.DataFrame, dept_length=int, door_length=int
) -> pd.DataFrame:
    """
    Adds column that represents logical segment break size in feet to the
    macro system output.

    Parameters
    ----------
    dept_length : int
        leght of opt legal break in inches
    door_length : int
        length of each door in inches
    pdf : pd.DataFrame
        macro system data output

    Returns
    -------
    macro output with logical_segment_size_ft column
    """

    # door_length and dept_length is in inch and dividing the inches by 12 to get ft
    pdf["logical_segment_size_ft"] = np.where(
        pdf["size_uom"] != "F", (door_length / 12), (dept_length / 12)
    )

    return pdf


def add_col_elasticity(
    pdf: pd.DataFrame,
    df_elasticity_lu: SparkDataFrame,
) -> pd.DataFrame:
    """
    Adds column that contains elasticity information to the macro system output

    Parameters
    ----------
    pdf : pd.DataFrame
        macro system data output

    df_elasticity_lu :

    Returns
    -------

    """

    dims = ["store_physical_location_id", "section_master"]
    dup_check_pd(pdf, dims)

    dims = ["STORE_PHYSICAL_LOCATION_NO", "SECTION_MASTER", "sales_or_margin"]
    dup_check(df_elasticity_lu, dims)

    data_definition = [
        ("STORE_PHYSICAL_LOCATION_NO", "store_physical_location_id", T.StringType()),
        ("SECTION_MASTER", "section_master", T.StringType()),
        ("sales_or_margin", "sales_or_margin", T.StringType()),
    ]

    # apply column renaming and casting while keeping only relevant columns
    df_facings_map_lookup = apply_data_definition(
        df=df_elasticity_lu,
        data_contract=data_definition,
        drop_if_not_in_data_contract=False,
    )

    cols = [x[1] for x in data_definition] + ["FACINGS_MAP"]
    df_facings_map_lookup = df_facings_map_lookup.select(*cols)

    df = spark.createDataFrame(pdf)

    # add in elasticity calculation for macro
    # this should probably be a list of tuples or name of a file with entire
    # curve used to be "TBD" originally
    dims = ["store_physical_location_id", "section_master", "sales_or_margin"]
    df = df.join(df_facings_map_lookup, dims, "left")
    df = backup_on_blob(spark, df)
    df = df.withColumnRenamed("FACINGS_MAP", "elasticity")

    # we are converting the the elasticity column (the nested map) to a string
    # in order to avoid Spark -> Pandas conversion introducing new decimal
    # places + we won't need the map itself any more downstream
    df = df.withColumn("elasticity", F.col("elasticity").cast(T.StringType()))
    pdf = df.toPandas()

    return pdf


def prepare_data_for_output_macro(pdf: pd.DataFrame) -> SparkDataFrame:
    """
    Prepares the macro dataset for the final output by:
     - selecting only relevant columns
     - performing final type conversion
     - cleaning up elasticity column to make sure it is a nice JSON string

    Parameters
    ----------
    pdf : pd.DataFrame
        macro system data output


    Returns
    -------
    final macro system output
    """

    data_def = [
        ("unique_items_opt_assorted", "optimal_unique_items", int),
        ("unique_items_cur_assorted", "current_unique_items", int),
        ("store_cluster_id", "store_cluster_id", str),
        ("section_master", "category", str),
        ("department", "department", str),
        ("store_physical_location_id", "store_physical_location_id", str),
        ("store_no", "store_no", str),
        ("segmentation_cluster", "segmentation_cluster", str),
        ("sales_or_margin", "sales_or_margin", str),
        ("logical_segment_size_ft", "logical_segment_size_ft", int),
        ("size_uom", "size_uom", str),
        ("optimal_size", "optimal_size", str),
        ("current_size", "current_size", str),
        ("opt_sales", "optimal_value", int),
        ("cur_sales", "current_value", int),
        ("current_facings", "current_facings", int),
        ("SECTION_REMOVAL_ORDER", "reduce_rank", int),
        ("SECTION_ADDITION_ORDER", "increase_rank", int),
        ("expected_publish_date", "expected_publish_date", str),
        ("run_name", "run_name", str),
        ("run_id", "run_id", str),
        ("run_datetime", "run_datetime", str),
    ]

    pdf = apply_data_definition_pd(
        pdf=pdf,
        data_contract=data_def,
        drop_if_not_in_data_contract=True,
    )

    # other type conversions
    pdf["optimal_value"] = (
        pdf["optimal_value"].astype(float).round(decimals=0).astype(int)
    )

    pdf["optimal_size"] = (
        pdf["optimal_size"].astype(float).round(decimals=0).astype(int)
    )
    pdf["current_size"] = pdf["current_size"].astype(str)

    df_macro_output = spark.createDataFrame(pdf)

    cols = [
        (
            x.name,
            x.name,
            x.dataType,
        )
        for x in dc.macro.schema
    ]

    df_macro_output = apply_data_definition(
        df=df_macro_output,
        data_contract=cols,
    )

    return df_macro_output


def add_col_optimal_size_and_current_size(pdf: pd.DataFrame):
    """
    adding 'optimal_size' and 'current_size' as a product of # of breaks
    and the increment width.
    For departments that use "doors" ("D"), we only take the # of breaks
    """

    mask = pdf["size_uom"] == "D"
    col_width = pdf["legal_increment_width"]
    col_optimal_size_other = pdf["opt_legal_breaks"] * col_width
    col_optimal_size_d = pdf["opt_legal_breaks"]
    col_current_size_other = pdf["cur_legal_breaks"] * col_width
    col_current_size_d = pdf["cur_legal_breaks"]
    col_optimal_size = np.where(mask, col_optimal_size_d, col_optimal_size_other)
    col_current_size = np.where(mask, col_current_size_d, col_current_size_other)
    pdf["optimal_size"] = col_optimal_size
    pdf["current_size"] = col_current_size
    return pdf


def add_col_item_counts(
    pdf: pd.DataFrame,
    df_opt_output_with_store_cluster_id: SparkDataFrame,
    df_system_micro_output: SparkDataFrame,
):
    """
    adds columns to the macro output:
    'unique_items_cur_assorted' - number of unique items currently (before opt)
    'unique_items_opt_assorted' - number of unique items after opt
    by each store/SM
    """

    # shortcuts
    df_opt = df_opt_output_with_store_cluster_id
    df_mic = df_system_micro_output

    # subset only to the micro scope to ensure counts are consistent
    dims = ["item_no", "store_cluster_id"]
    df_scope = df_mic.select(*dims).dropDuplicates()
    df_opt = df_opt.join(df_scope, dims, "inner")

    # where the current facings are positive, take the distinct item count
    # where the optimal facings are positive, take the distinct item count
    dims = ["store_physical_location_id", "section_master"]
    mask_cur = F.col("current_facings") > 0
    mask_opt = F.col("optim_facings") > 0
    agg_cur = [F.countDistinct("item_no").alias("unique_items_cur_assorted")]
    agg_opt = [F.countDistinct("item_no").alias("unique_items_opt_assorted")]
    df_cur = df_opt.filter(mask_cur).groupBy(*dims).agg(*agg_cur)
    df_opt = df_opt.filter(mask_opt).groupBy(*dims).agg(*agg_opt)

    # join them in and fillna with zero - no match will mean no item count
    pdf_cur = df_cur.toPandas()
    pdf_opt = df_opt.toPandas()
    dims = ["store_physical_location_id", "section_master"]
    pdf = pdf.merge(pdf_cur, "left", dims)
    pdf = pdf.merge(pdf_opt, "left", dims)
    pdf["unique_items_cur_assorted"] = pdf["unique_items_cur_assorted"].fillna(0)
    pdf["unique_items_opt_assorted"] = pdf["unique_items_opt_assorted"].fillna(0)

    return pdf


def add_col_increase_decrease_rank(
    pdf: pd.DataFrame,
    df_rem: SparkDataFrame,
    df_add: SparkDataFrame,
    df_scm: SparkDataFrame,
    df_opt: SparkDataFrame,
):
    """
        Adds increase/decrease rank for the macro file
        using the store_cluster_id mapping.

        Macro file is on store level, whereas ranking data is on store_cluster_id
        level.
        Since each store_cluster_id is a combination of cluster/department
        we could use store_cluster_id -> store mapping table to join in
        store_cluster_id on department / store level, which is used to bring
        store_cluster_id into the macro file.

        Parameters
        ----------
        pdf : pd.DataFrame
            macro dataset
    `
        df_rem : SparkDataFrame
            removals ranking dataset

        df_add : SparkDataFrame
            additions ranking dataset

        df_scm : SparkDataFrame
            store-cluster mapping system file dataset

        df_opt : SparkDataFrame
            optimization output

        Returns
        -------
        macro dataset with increase/reduce ranking
    """

    df = spark.createDataFrame(pdf)

    #
    df_dep_lookup = df_opt.select("section_master", "department").dropDuplicates()
    dup_check(df_dep_lookup, ["section_master"])
    df = df.join(df_dep_lookup, ["section_master"], "left")
    df_check = df.filter(is_col_null_mask("department"))
    assert df_check.limit(1).count() == 0, "bad 'department' lookup"

    dims = ["department", "store_physical_location_id", "category"]
    df_sci_lookup = df_scm.select(dims + ["store_cluster_id"]).dropDuplicates()
    dup_check(df_sci_lookup, dims)
    df = df.withColumnRenamed("section_master", "category")
    df = df.join(df_sci_lookup, dims, "left")
    df = df.withColumnRenamed("category", "section_master")
    df_check = df.filter(is_col_null_mask("store_cluster_id"))
    assert df_check.limit(1).count() == 0, "bad 'store_cluster_id' lookup"

    # dup checks
    dims = ["STORE_CLUSTER_ID"]
    dup_check(df_rem, dims)
    dup_check(df_add, dims)

    # join in the removals ranking
    cols_rem = dims + ["SECTION_REMOVAL_ORDER"]
    df = df.join(df_rem.select(*cols_rem), dims, "left")
    df_check = df.filter(is_col_null_mask("SECTION_REMOVAL_ORDER"))
    assert df_check.limit(1).count() == 0, "bad 'SECTION_REMOVAL_ORDER' lookup"

    # join in the additions ranking
    cols_add = dims + ["SECTION_ADDITION_ORDER"]
    df = df.join(df_add.select(*cols_add), dims, "left")
    df_check = df.filter(is_col_null_mask("SECTION_ADDITION_ORDER"))
    assert df_check.limit(1).count() == 0, "bad 'SECTION_ADDITION_ORDER' lookup"

    pdf_result = df.toPandas()

    return pdf_result


def add_store_cluster_id_attributes_from_opt(
    pdf: pd.DataFrame,
    df_opt_output_with_store_cluster_id: SparkDataFrame,
    cols_store_cluster_id_attributes: List[str],
):
    df = df_opt_output_with_store_cluster_id

    col = F.col("category_size").cast(T.IntegerType())
    df = df.withColumn("category_size", col)

    msg = f"""
    The dataset already contains 1 or more of these 'store_cluster_id' 
    attributes: {cols_store_cluster_id_attributes}
    Check your columns
    """

    assert all(
        [x not in list(pdf.columns) for x in cols_store_cluster_id_attributes]
    ), msg

    cols = ["store_cluster_id"] + cols_store_cluster_id_attributes
    df_lookup = df.select(*cols).dropDuplicates()

    msg = """
    Since you are adding 'store_cluster_id' attributes there should
    only be 1 value of any attribute for each 'store_cluster_id' attributes.
    This dup check checks for that and failed. Please check your
    dimensionality
    """

    dup_check(df=df_lookup, dims=["store_cluster_id"], assert_=True, msg=msg)

    pdf_lookup = df_lookup.toPandas()

    pdf = pdf.merge(right=pdf_lookup, how="left", on=["store_cluster_id"])

    # check that the join is clean
    msg = f"""
    Some of the 'store_cluster_id' columns were not joined in
    cleanly: {cols_store_cluster_id_attributes}
    """

    check = all([not pdf[x].isnull().any() for x in cols_store_cluster_id_attributes])
    assert check, msg

    return pdf


def replicate_and_override_records_with_data_from_max_sales(
    pdf: pd.DataFrame,
    col_max_sales: str,
    dims_general: List[str],
    dims_replication: List[str],
    dims_each_replication_slice: List[str],
    dims_original: List[str],
    cols_attributes_to_replicate: List[str],  # data columns (yellow)
    lookup_map: Dict[str, List[str]],
) -> pd.DataFrame:
    """
    Overrides data in neighbouring slices of 'dims_replication' columns
    for 1 slice of 'dims_replication' that has highest sales among all
    'dims_replication' slices for each 'dims_general' slice.

    In other words, it replicates (copies over) the data from a highest-sales
    "child" array of data into every other "child" array of data which happens
    within a "parent" array of data.

    For e.g. this is needed to replicate data from:
    - highest-sales slice of department_size/size_uom into the rest of
      slices of department_size/size_oum (dims_replication)
    - within each region/banner/dept/cluster (dims_general)
    - to ensure that assortment, i.e. item_no and rest of the data is
      consistent (cols_attributes_to_replicate with dimensionality specified
      by dims_each_replication_slice)

    This function has a lot of asserts and dup checks, they are necessary
    not ensure the right behaviour. Please note, it is possible to feed
    bad data into this function (e.g. where the hierarchy between
    dims_general -> dims_replication -> dims_each_replication_slice
    is not clean), which will be caught by these asserts. Thus it is important
    to keep these asserts and dup checks.

    ----------
    pdf : pd.DataFrame
        main dataframe that needs data replication / overwriting

    col_max_sales : str
        column name that represents sales that need to be used (max)

    dims_general : List[str]
        data will be replicated WITHIN each slice of this dimensionality

    dims_replication : List[str]
        data will be replicated ACROSS each slice of this dimensionality

    dims_each_replication_slice : List[str]
        each slice of data defined by dims_general + dims_replication
        TOGETHER must be unique on this dimensionality

    dims_original : List[str]
        original dimensionality of the data. Is not used in replication
        logic, only used to validate that it still holds after the operation

    cols_attributes_to_replicate : List[str]
        list of columns-attributes that need to be replicated / overwritten

    lookup_map : Dict[str, List[str]]
        a mapping between column name and list of dimensions used to lookup
        the values in this column. Used at the end to bring back any data
        that should not be replicated but should remain as-is.

    Returns
    -------
    dataset with data replicated / overwritten across dims_replication
    for each slice of dims_general
    """

    dup_check_pd(pdf, dims_original)

    # first we do some validation of inputs
    # ensure that attributes that need to be replicated don't
    # contain the sales column that we use to find best slice as well
    # as any other dimensions

    l_check = list(set(cols_attributes_to_replicate).intersection(dims_general))
    msg = f"cols_attributes_to_replicate intersects with general dims: {l_check}"
    assert len(l_check) == 0, msg

    l_check = list(set(cols_attributes_to_replicate).intersection(dims_replication))
    msg = f"cols_attributes_to_replicate intersects with replication dims: {l_check}"
    assert len(l_check) == 0, msg

    msg = f"sales col ('{col_max_sales}') cannot be in attributes"
    assert col_max_sales not in cols_attributes_to_replicate, msg

    # ensure that general and replication dimensions are different columns
    l_check = list(set(dims_general).intersection(dims_replication))
    msg = f"general and replication dimensions cannot intersect: {l_check}"
    assert len(l_check) == 0, msg

    # we first aggregate on BOTH the general and replication dimensions
    # to be able to calcualte the sum of sales for each slice inside
    # replication dimensions
    dims = dims_general + dims_replication
    agg = {"opt_sales": pd.NamedAgg("opt_sales", "sum")}
    pdf_agg = pdf.groupby(dims).agg(**agg).reset_index()

    # now we apply the ranking
    # we are sorting DESC by sales, and than DESC by replication dimensions
    # in case if there is a tie
    # this is done within each general dimension
    cols_sorting = dims_general + [col_max_sales] + dims_replication
    asc = [True for x in dims_general] + [False] + [False for x in dims_replication]
    col_rank = pdf_agg.sort_values(
        by=cols_sorting, axis=0, ascending=asc, inplace=False
    )
    col_rank = col_rank.groupby(dims_general).cumcount() + 1
    pdf_agg["rank"] = col_rank

    # at this step the dimensionality should be on both
    # general + replication dimensions
    dims = dims_general + dims_replication
    dup_check_pd(pdf_agg, dims)

    # pdf_agg.query("region=='ontario' and banner=='SOBEYS' and department=='Frozen' and segmentation_cluster=='2B' and optimal_size==10")
    # pdf_agg.query("region=='ontario' and banner=='SOBEYS' and department=='Frozen' and segmentation_cluster=='2B' and optimal_size==8")

    # filter down only to 1 slice in each general dimension
    # where the sales rank is 1
    pdf_agg_best = pdf_agg.query("rank==1")

    # now it should be unique on general dims because we picked 1 replication
    # slice from each general dimension
    dup_check_pd(pdf_agg_best, dims_general)

    # lookup the rest of the data for our best slice
    # NOTE: this lookup is expected to explode because the right side
    # can contain item_no and other dimensions that we WANT to override

    # we don't want to include sales column so lets break up columns
    # into dimensions and attributes that we want and only include those
    # in the 'right' dataset
    # and perform the explosion join
    dims = dims_general + dims_replication
    cols_attr = cols_attributes_to_replicate
    pdf_agg_best_with_attr = pdf_agg_best.merge(
        right=pdf[dims + cols_attr], on=dims, how="inner"
    )

    # this explosion should only add dimensions from each replication
    # slice as part of the explosion
    dup_check_pd(pdf_agg_best_with_attr, dims_general + dims_each_replication_slice)

    # now we will do replication to the rest of the replication dimensions
    # by joining ONLY on general dimensions.
    # NOTE: this way we will also get an explosion because we want to
    # replicate the data from 'best' PDF to rest of the replication dimensions

    # first we create a "skeleton" using the general + replication dimensions
    dims = dims_general + dims_replication
    pdf_skeleton = pdf[dims].drop_duplicates()

    # we don't want the replication dimensions from the 'right' datset
    # because those are from the 'best' one, and should be overwritten,
    # therefore exclude them by only selecting required columns
    cols_right = dims_general + cols_attributes_to_replicate + [col_max_sales]

    # next we join ONLY on general dimensions to ensure it explodes
    pdf_result = pdf_skeleton.merge(
        right=pdf_agg_best_with_attr[cols_right],
        on=dims_general,
        how="inner",
    )

    dims = dims_general + dims_replication + dims_each_replication_slice
    dup_check_pd(pdf_result, dims)

    # data replication is now done

    # lastly we need to look up back some of the columns that were in
    # the original 'pdf' dataset but we did not account for them during the
    # process usually these are dimensional columns

    for col_name, dims in lookup_map.items():
        # first ensure that
        msg = f"Column to lookup '{col_name}' is already in data"
        assert col_name not in pdf_result.columns, msg

        msg = f"Some column lookup dims don't exist in result: {dims}"
        assert all([x in pdf_result.columns for x in dims]), msg

        pdf_l = pdf[dims + [col_name]].drop_duplicates()
        dup_check_pd(pdf_l, dims)

        pdf_result = pdf_result.merge(
            right=pdf_l,
            how="left",
            on=dims,
        )

        # ensure the join was clean
        msg = f"Bad lookup of '{col_name}' usin '{dims}'"
        assert not pdf_result[col_name].isnull().any(), msg

    # do a final assert to ensure we did not lose any columns
    cols_lost = list(set(pdf.columns) - set(pdf_result.columns))
    msg = f"Some columns lost during process: {cols_lost}"
    assert len(cols_lost) == 0, msg

    # we cannot lose any combinations of general and/or replication dimensions
    # compare to see that dimension set is has not changed
    pdf_dims_orig = pdf[dims_general + dims_replication].drop_duplicates()
    pdf_dims_rslt = pdf_result[dims_general + dims_replication].drop_duplicates()
    pdf_dims_orig["in_orig"] = 1
    pdf_dims_rslt["in_rslt"] = 1

    pdf_check = pdf_dims_orig.merge(
        right=pdf_dims_rslt,
        how="outer",
        on=dims_general + dims_replication,
    )

    msg = f"Dimensions are not matching between the original and final datasets"
    assert len(pdf_check.query("in_orig!=1 or in_rslt!=1")) == 0, msg

    # final dup check on the original dimensions again (MUST STILL HOLD)
    dup_check_pd(pdf_result, dims_original)

    return pdf_result
