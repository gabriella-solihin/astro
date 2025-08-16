from typing import List

import pandas as pd

from inno_utils.loggers.log import log
from spaceprod.utils.data_transformation import (
    apply_data_definition,
    apply_data_definition_pd,
    left_anti_join_pdfs,
    pyspark_df_col_to_unique_list,
)
from spaceprod.utils.imports import F, SparkDataFrame, T
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.validation import dup_check, dup_check_pd


def create_micro_skeleton(
    df_opt_facings_by_item_and_store_cluster_id: SparkDataFrame,
    df_opt_output_with_store_cluster_id: SparkDataFrame,
) -> pd.DataFrame:
    """
    Creates the skeleton for the micro output, i.e. only the dimensional data
    + some attributes

    Parameters
    ----------
    df_opt_facings_by_item_and_store_cluster_id : output of the optimization
    df_opt_output_with_store_cluster_id : opt output with store_cluster_id

    Returns
    -------
    skeleton dataset for the micro system file
    """

    # convert inputs to pandas for more efficient processing as the data is
    # is not a granular micro data
    pdf_opt_sci = df_opt_facings_by_item_and_store_cluster_id.toPandas()
    pdf_opt = df_opt_output_with_store_cluster_id.toPandas()

    # quick dup checks to make sure inputs are valid
    dims = ["store_cluster_id", "item_no"]
    dup_check_pd(pdf_opt_sci, dims)
    dims = ["store_cluster_id", "item_no", "store_physical_location_id"]
    dup_check_pd(pdf_opt, dims)

    cols_from_opt_sci = [
        "store_cluster_id",  # dim
        "item_no",  # dim
        "need_state",
        "optim_facings",
        "current_facings",
    ]

    cols_from_opt = [
        "store_cluster_id",  # dim
        "item_no",  # dim
        "section_master",
        "item_name",
        "need_state_idx",
    ]

    # we take some of the store_cluster_id / item -level attributes from the
    # opt store-level output and ensure they are consistent across stores
    # by doing a dup check
    pdf_opt = pdf_opt[cols_from_opt].drop_duplicates()
    dims = ["store_cluster_id", "item_no"]
    dup_check_pd(pdf_opt, dims)

    # combine together the store_cluster_id / item -level data with
    # the attributes we got from opt output
    dims = ["store_cluster_id", "item_no"]
    pdf = pdf_opt_sci[cols_from_opt_sci].merge(right=pdf_opt, on=dims, how="inner")

    # do the final dup check to make sure dimensionality still holds
    dims = ["store_cluster_id", "item_no"]
    dup_check_pd(pdf, dims)

    return pdf


def add_col_sales_or_margin(pdf: pd.DataFrame) -> pd.DataFrame:
    """adds sales_or_margin column to the micro system output"""
    # add in sales or margin label
    # TODO: depends on item's category.
    #  We need to get cat<->sales_or_margin mapping
    pdf["sales_or_margin"] = "sales"
    return pdf


def add_col_needstate_sku_idx(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    adds needstate_sku_idx column to the micro system output
    indicates the recommended order of SKUs within the Need States
    """
    # add in needstate_SKU_idx
    # currently the recommended order of SKUs within Section_Master / NS is
    # alphabetically by item name  <- TODO: this ordering logic might change

    # first create a lookup that will be on NS / item level and create the
    # ranking there

    dims = ["section_master", "need_state_idx"]
    cols_sort = dims + ["item_name", "item_no"]
    pdf_lookup = pdf[cols_sort].drop_duplicates()
    pdf_lookup = pdf_lookup.sort_values(by=cols_sort)
    pdf_lookup = pdf_lookup.reset_index(drop=True)
    pdf_lookup["needstate_sku_idx"] = pdf_lookup.groupby(dims).cumcount() + 1

    # check the dups and join
    dims = ["section_master", "need_state_idx", "item_no"]
    dup_check_pd(pdf_lookup, dims)
    cols_req = dims + ["needstate_sku_idx"]
    pdf = pdf.merge(pdf_lookup[cols_req], "left", dims)

    # check that the join was clean
    msg = "bad 'needstate_sku_idx' lookup"
    assert not pdf["needstate_sku_idx"].isnull().any(), msg

    # drop item_name column as it is not needed in the output
    pdf = pdf.drop("item_name", axis=1, inplace=False, errors="raise")

    return pdf


def add_col_ranks(
    pdf: pd.DataFrame,
    df_ranking_removal: SparkDataFrame,
    df_ranking_addition: SparkDataFrame,
    df_store_cluster_mapping: SparkDataFrame,
) -> pd.DataFrame:
    """adds reduce/increase rank column to the micro system output"""

    dims = ["store_physical_location_id", "store_cluster_id"]
    dup_check(df_store_cluster_mapping, dims)
    dup_check(df_ranking_removal, ["STORE_CLUSTER_ID", "ITEM_TO_REMOVE"])
    dup_check(df_ranking_addition, ["STORE_CLUSTER_ID", "ITEM_TO_ADD"])

    data_def_rem = [
        ("STORE_CLUSTER_ID", "store_cluster_id", T.StringType()),
        ("ITEM_TO_REMOVE", "item_no", T.StringType()),
        ("ORDER", "reduce_rank", T.IntegerType()),
    ]

    data_def_add = [
        ("STORE_CLUSTER_ID", "store_cluster_id", T.StringType()),
        ("ITEM_TO_ADD", "item_no", T.StringType()),
        ("ORDER", "increase_rank", T.IntegerType()),
    ]

    df_ranking_lookup_rem = apply_data_definition(
        df=df_ranking_removal,
        data_contract=data_def_rem,
        drop_if_not_in_data_contract=True,
    )

    df_ranking_lookup_add = apply_data_definition(
        df=df_ranking_addition,
        data_contract=data_def_add,
        drop_if_not_in_data_contract=True,
    )

    pdf_ranking_lookup_rem = df_ranking_lookup_rem.toPandas()
    pdf_ranking_lookup_add = df_ranking_lookup_add.toPandas()

    dim = ["store_cluster_id", "item_no"]
    dup_check_pd(pdf_ranking_lookup_rem, dim)
    dup_check_pd(pdf_ranking_lookup_add, dim)

    # merge in the ranking info

    pdf = pdf.merge(right=pdf_ranking_lookup_rem, how="inner", on=dim)

    pdf = pdf.merge(right=pdf_ranking_lookup_add, how="inner", on=dim)

    return pdf


def add_col_metadata(
    pdf: pd.DataFrame,
    run_id: str,
    run_date_time: str,
    expected_publish_date: str,
    run_name_prefix: str,
) -> pd.DataFrame:
    """
    adds the metadata to the dataset as per specs from contactix to identify
    the current release of the data
    Parameters
    ----------
    pdf : str
        micro system dataset

    run_id : str
        duplication of the run_id into each output table enables this
        information to be linked to POG data in CKB and to supplier and
        category constraints maintained in sharepoint.
        currently using our inter internal run id (Space Prod)

    run_date_time : str
        datetime stamp for model run that generated output

    expected_publish_date : str
        date when POG generated from this data is expected to be published

    run_name_prefix :
        defaults to run_id if not provided, this is a display name that can
        be used for referencing a specific spaceprod model run event,
        e.g. “post-negotiation-2023-reline-v1”

    Returns
    -------
    micro output with metadata
    """
    pdf["run_id"] = run_id
    pdf["run_datetime"] = run_date_time
    pdf["expected_publish_date"] = expected_publish_date
    pdf["run_name"] = f"{run_name_prefix}_{run_id}"

    return pdf


def prepare_data_for_output_micro(pdf: pd.DataFrame) -> SparkDataFrame:
    """selects only relevant columns and coverts back to PySpark DF"""

    # Select and sort to just cols we need
    cols_mapping = [
        ("store_cluster_id", "store_cluster_id", str),
        ("section_master", "category", str),
        ("item_no", "item_no", str),
        ("sales_or_margin", "sales_or_margin", str),
        ("need_state_idx", "needstate_idx", str),
        ("needstate_sku_idx", "ideal_adjacency", int),
        ("optim_facings", "optimal_facings", int),
        ("current_facings", "current_facings", int),
        ("reduce_rank", "reduce_rank", int),
        ("increase_rank", "increase_rank", int),
        ("expected_publish_date", "expected_publish_date", str),
        ("run_name", "run_name", str),
        ("run_id", "run_id", str),
        ("run_datetime", "run_datetime", str),
    ]

    pdf = apply_data_definition_pd(
        pdf=pdf,
        data_contract=cols_mapping,
        drop_if_not_in_data_contract=False,
    )

    # covert back to PySpark DF for compatibility
    df_micro_output = spark.createDataFrame(pdf)

    return df_micro_output


def get_run_date_time_from_df(df: SparkDataFrame) -> str:
    """
    gets the run date-time from a DF
    we should not re-create it to make sure it is consistent
    throughout all system files
    """

    list_ = pyspark_df_col_to_unique_list(df, "run_datetime")
    msg = f"More than 1 'run_datetime' found in data: {list_}"
    assert len(list_) == 1, msg
    return list_[0]


def add_col_store_physical_location_id_and_explode(
    pdf: pd.DataFrame,
    df_store_cluster_mapping: SparkDataFrame,
) -> pd.DataFrame:
    """
    explodes the micro output to include store dimension
    """

    dims = ["store_physical_location_id", "store_cluster_id"]
    dup_check(df_store_cluster_mapping, dims)

    dims = ["store_cluster_id", "item_no"]
    dup_check_pd(pdf, dims)

    cols = ["store_physical_location_id", "store_cluster_id"]
    pdf_store_lookup = df_store_cluster_mapping.select(*cols).toPandas()

    # this join WILL EXPLODE which is expected
    pdf_result = pdf.merge(pdf_store_lookup, "left", "store_cluster_id")

    # check to ensure a clean join
    msg = "bad 'store_physical_location_id' lookup"
    assert not pdf_result["store_physical_location_id"].isnull().any(), msg

    return pdf_result


def add_col_opt_sales(
    pdf: pd.DataFrame,
    df_opt_output_with_store_cluster_id: SparkDataFrame,
):
    dup_check_pd(pdf, ["store_cluster_id", "item_no"])
    dup_check(
        df_opt_output_with_store_cluster_id, ["item_no", "store_physical_location_id"]
    )

    dims = ["store_cluster_id", "item_no"]
    agg = [F.sum("opt_sales").alias("opt_sales")]
    df_sales = df_opt_output_with_store_cluster_id.groupBy(*dims).agg(*agg)
    pdf_sales = df_sales.toPandas()

    dims = ["store_cluster_id", "item_no"]
    pdf = pdf.merge(right=pdf_sales, on=dims, how="left")
    assert not pdf["opt_sales"].isnull().any(), "bad sales lookup"

    return pdf


def filter_to_section_masters_in_scope(
    df: SparkDataFrame,
    list_section_masters: List[str],
    col_section_master: str,
) -> SparkDataFrame:
    """
    Filters given data to list of section masters (if supplied)
    Parameters
    ----------
    df : SparkDataFrame
        data to filter to given section_master's

    list_section_masters: List[str]
        list of section masters to filter to

    col_section_master : str
        name of the column to filter on (if empty, will not filter)

    Returns
    -------
    filtered data
    """

    if len(list_section_masters) == 0:
        msg = "No need to filter BY data to Section Masters"
        log.info(msg)
        return df

    msg = f"Filtering BY data to the following Section Masters: {list_section_masters}"
    log.info(msg)
    mask = F.col(col_section_master).isin(list_section_masters)
    df_result = df.filter(mask)

    # make sure we still have some data
    msg = f"After filtering to these SMs, no data left, check SM spelling"
    assert df_result.limit(1).count() > 0, msg

    return df_result
