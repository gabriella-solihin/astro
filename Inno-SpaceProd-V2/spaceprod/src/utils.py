"""
Contains business-logic-related utils, e.g. commonly-used operations on
data that is shared across ALL modules.

i.e. things that are widely used across ALL module
For e.g.:
 - de-duping product table/location table is used 99% of modules
 - merging external store data with location table

These things must be done in a consistent way, thus we are defining these
functions to be used where needed

"""
from typing import List, Union

from inno_utils.loggers import log
from spaceprod.utils.data_transformation import union_dfs
from spaceprod.utils.decorators import timeit
from spaceprod.utils.imports import F, SparkDataFrame, T
from spaceprod.utils.validation import dup_check


def filter_out_inactive_locations(
    df_location: SparkDataFrame,
    col_store_entity: Union[str, List[str]],
) -> SparkDataFrame:
    """
    Filters out locations that are considered "inactive" from the 'location'
    table.

    It ensures that in case if ALL records in a given store are inactive
    the store is not filtered out completely. This is needed later when we
    dedup on location data -> if there is a choice between active/inactive
    the logic will never pick inactive (unless there is no choice).

    To identify which store entities are of interest, the parameter
    'col_store_entity' specifies the column name(s) that represent a store
    (similar to `dedup_location` function).

    NOTE: it should be called BEFORE dedup_location.

    Parameters
    ----------
    df_location : SparkDataFrame
        location table (external)

    col_store_entity:
        the identifier of the store you are interested in, e.g.
        STORE_NO, STORE_PHYSICAL_LOCATION_NO, etc.
        The resulting dataset will contain ALL records from this column(s).
        The function ensures that none of these entities are dropped during
        filtering

    Returns
    -------
    filtered location table
    """

    df_loc = df_location

    # data should already be unique on SK
    dup_check(df_loc, ["RETAIL_OUTLET_LOCATION_SK"])

    # in case if only 1 column is supplied as string, make sure its a list
    cols = [col_store_entity] if isinstance(col_store_entity, str) else col_store_entity

    # get stores for which ALL records are inactive.
    # we don't want to filter these out so that we don't lose stores
    agg = [F.collect_set(F.col("ACTIVE_STATUS_CD")).alias("STATUSES")]
    df_statuses = df_loc.groupBy(*cols).agg(*agg)

    # now filter only to those stores that contain ONLY inactive SK records
    mask_only_inactive = F.col("STATUSES") == F.array([F.lit("I")])
    df_only_inactive_list = df_statuses.filter(mask_only_inactive)
    df_only_inactive_list = df_only_inactive_list.drop("STATUSES")
    df_only_inactive = df_loc.join(df_only_inactive_list, cols, "inner")

    # get remaining stores and only include active records for those
    df_remain = df_loc.join(df_only_inactive_list, cols, "left_anti")
    mask = F.col("ACTIVE_STATUS_CD") != "I"
    df_remain_active = df_remain.filter(mask)

    # combine the two slices of location data together
    df = union_dfs([df_remain_active, df_only_inactive], align_schemas=True)

    return df


def dedup_location(
    df_location: SparkDataFrame,
    col_store_entity: Union[str, List[str]],
) -> SparkDataFrame:
    """
    Dedups the store data by keeping the LATEST record for each
    'col_store_entity'.

    When store data is updated, the records are not being updtaed, instead
    a new record is inserted while incrementing
    RETAIL_OUTLET_LOCATION_SK

    Parameters
    ----------
    df_location:
        location external table

    col_store_entity:
        the identifier of the store you are interested in, e.g.
        STORE_NO, STORE_PHYSICAL_LOCATION_NO, etc.
        The resulting dataset will contain ALL records from this column
        the rest of the columns will use the 'latest' data


    Returns
    -------
    de-duped location table with ALL original columns
    """

    # data should already be unique on SK
    dup_check(df_location, ["RETAIL_OUTLET_LOCATION_SK"])

    # in case if only 1 column is supplied as string, make sure its a list
    cols = [col_store_entity] if isinstance(col_store_entity, str) else col_store_entity

    # get the latest <col_store_entity> record for each store
    # the latest is the one that has the highest key (RETAIL_OUTLET_LOCATION_SK)
    col = "RETAIL_OUTLET_LOCATION_SK"
    agg = [F.max(F.col(col).cast(T.IntegerType())).cast(T.StringType()).alias(col)]
    df_latest_loc = df_location.groupBy(*cols).agg(*agg)

    # filter only to latest records
    dims = cols + ["RETAIL_OUTLET_LOCATION_SK"]
    df_loc = df_latest_loc.join(df_location, dims, "inner")

    return df_loc


def process_location(
    df_location: SparkDataFrame,
    col_store_entity: Union[str, List[str]],
) -> SparkDataFrame:
    """
    a wrapper around
    - filter_out_inactive_locations(...)
    - dedup_location(...)

    processes 'df_location' (external) table to include valid records
    for the given set of location entities ('col_store_entity')

    please refer to corresponding docstrings of each function to understand
    the processing logic

    Parameters
    ----------
    df_location : SparkDataFrame
        location table (external)

    col_store_entity:
        the identifier of the store you are interested in, e.g.
        STORE_NO, STORE_PHYSICAL_LOCATION_NO, etc.
        The resulting dataset will contain ALL records from this column(s).
        The function ensures that none of these entities are dropped during
        processing

    Returns
    -------
    processed location table
    """

    df = filter_out_inactive_locations(
        df_location=df_location,
        col_store_entity=col_store_entity,
    )

    df = dedup_location(
        df_location=df,
        col_store_entity=col_store_entity,
    )

    return df


def dedup_product(
    df_product: SparkDataFrame,
    col_product_entity: Union[str, List[str]],
) -> SparkDataFrame:
    """
    Dedups the product data by keeping the LATEST record for each
    'col_product_entity'.

    When product data is updated, the records are not being updated, instead
    a new record is inserted while incrementing ITEM_SK

    Parameters
    ----------
    df_product:
        product external table

    col_product_entity:
        the identifier of the product you are interested in, e.g.
        ITEM_NO, UPC, etc.


    Returns
    -------
    de-duped location table with ALL original columns
    """

    # in case if only 1 column is supplied as string, make sure its a list
    cols = (
        [col_product_entity]
        if isinstance(col_product_entity, str)
        else col_product_entity
    )

    # get the latest <col_product_entity> record for each store
    # the latest is the one that has the highest key (ITEM_SK)=
    col = "ITEM_SK"
    agg = [F.max(F.col(col).cast(T.IntegerType())).cast(T.StringType()).alias(col)]
    df_latest_prod = df_product.groupBy(*cols).agg(*agg)

    # filter only to latest records
    dims = cols + ["ITEM_SK"]
    df_prod = df_product.join(df_latest_prod, dims, "inner")

    return df_prod


@timeit
def spit_run_summary():
    """
    a util that produces a quick human-readable summary of the run for
    sharing
    """

    from spaceprod.utils.space_context import context

    # determine which config(s) we need for this task
    config_scope = context.config["scope"]
    run_folder = context.run_folder

    ###########################################################################
    # ACCESS THE REQUIRED CONFIG PARAMETERS
    ###########################################################################

    banners = config_scope["banners"]
    regions = config_scope["regions"]
    st_date = config_scope["st_date"]
    end_date = config_scope["end_date"]
    pog_section_list = config_scope["pog_section_masters"]

    list_key_outputs = {
        "POG_DATA": [
            "combined_pog_processed",
        ],
        "NEED STATES": [
            "final_need_states",
            "final_dist_matrix",
        ],
        "INTERNAL_CLUSTERING": [
            "clustering_output_assignment",
            "clustering_output_assignment",
            "combined_sales_levels",
            "combined_sales_store_section",
        ],
        "INTERNAL_PROFILING": [
            "section",
            "section_ns",
            "sale_summary_section",
            "sale_summary_need_state",
        ],
        "EXTERNAL_CLUSTERING": [
            "environics_output",
            "final_profiling_data",
            "feature_cluster",
        ],
        "EXTERNAL_PROFILING": [
            "profiling",
        ],
        "MERGED_CLUSTERS": ["merged_clusters"],
    }

    msg_data = ""

    for section_name, section_list in list_key_outputs.items():
        msg_data += f"\n{section_name}:\n"
        for dataset_id in section_list:

            # try reading data to ensure it exists before sharing
            # will break here if it does not exist
            context.data.read(dataset_id=dataset_id)

            # get the pat and append it to the list
            path = context.data.path(dataset_id=dataset_id)

            msg_data += f"\t- {path}\n"

        msg_data += "\n"

    msg = f"""
    The following run is complete. 
    
     
    
    Please see below the scope + list of key outputs:
     - st_date: {st_date}
     - end_date: {end_date}
     - pog_section_list: {pog_section_list or 'ALL'}
     - banners: {banners or 'ALL'}
     - regions: {regions or 'ALL'}
    
    Run folder: '{run_folder}'
    
    Key outputs:
    {msg_data}
     
    """

    log.info(msg)
