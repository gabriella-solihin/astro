from inno_utils.loggers import log
from spaceprod.src.utils import process_location
from spaceprod.utils.data_transformation import is_col_null_mask
from spaceprod.utils.imports import F, SparkDataFrame, T
from spaceprod.utils.validation import dup_check


def pre_process_mission_summary_data(
    df_mission_summ: SparkDataFrame,
    df_location: SparkDataFrame,
) -> SparkDataFrame:
    """
    Ensures that the mission summary dataset is valid and has no dups
    + brings the STORE_PHYSICAL_LOCATION_NO column to it

    Parameters
    ----------
    df_mission_summ: mission summary created upstream
    df_location: external location table

    Returns
    -------
    processed mission summary data
    """

    # dedup location and get the store pysical location to the shopping mission
    # dataset
    cols = ["STORE_NO", "STORE_PHYSICAL_LOCATION_NO"]
    df_location = process_location(df_location, "STORE_NO").select(*cols)
    df_mission_summ = df_mission_summ.join(df_location, "STORE_NO", "left")
    dup_check(df_mission_summ, ["STORE_PHYSICAL_LOCATION_NO"])
    return df_mission_summ


def pre_process_store_list_data(df_store_list: SparkDataFrame) -> SparkDataFrame:
    """
    Ensures that the store list dataset is valid and has no dups
    Parameters
    ----------
    df_store_list

    Returns
    -------
    processed store list data
    """

    # it is known that the only 1 dup that this data has is for this record.
    # lets explicitly exclude it
    mask = (F.col("BANNER_KEY") == "5003801") & (F.col("PARENT_DESC") == "UNKNOWN")
    df_store_list = df_store_list.filter(~mask)
    dup_check(df_store_list, ["BANNER_KEY"])

    return df_store_list


def produce_file_1_view(
    df_merge_clusters: SparkDataFrame,
    df_store_list_processed: SparkDataFrame,
    df_mission_summ_processed: SparkDataFrame,
):
    """
    Creates the "File 1" view for the dashboard based on 3 main datasets
    Parameters
    ----------
    df_merge_clusters: merged clusters output created upstream
    df_store_list_processed: processed store list data
    df_mission_summ_processed: processed location data

    Returns
    -------
    "File 1" view data for dashboard
    """

    # quick dup check before joining
    dup_check(df_merge_clusters, ["BANNER_KEY", "REGION", "BANNER_KEY"])

    # here we only select columns that are need in File 1
    cols = [
        df_merge_clusters["BANNER"].alias("BANNER_ENGINE"),
        df_merge_clusters["REGION"].alias("REGION_ENGINE"),
        df_merge_clusters["BANNER_KEY"],
        df_merge_clusters["STORE_PHYSICAL_LOCATION_NO"],
        df_store_list_processed["BAN_NAME"],
        df_store_list_processed["BAN_NO"],
        df_store_list_processed["PARENT_DESC"],
        df_store_list_processed["BAN_DESC"],
        df_store_list_processed["CITY"],
        df_store_list_processed["LATITUDE"],
        df_store_list_processed["LONGITUDE"],
        df_store_list_processed["tot_gross_area"].alias("TOT_GROSS_AREA"),
        df_merge_clusters["INTERNAL_CLUSTER"],
        df_merge_clusters["EXTERNAL_CLUSTER"],
        df_merge_clusters["MERGED_CLUSTER"].alias("MERGE_CLUSTER"),
        df_mission_summ_processed["TOT_VISITS"],
        df_mission_summ_processed["TOT_UNITS"],
        df_mission_summ_processed["TOT_SALES"],
        df_mission_summ_processed["TOT_PROMO_SALES"],
        df_mission_summ_processed["TOT_PROMO_UNITS"],
        df_mission_summ_processed["SPEND_PER_VISIT"],
        df_mission_summ_processed["UNITS_PER_VISIT"],
        df_mission_summ_processed["PRICE_PER_UNIT"],
        df_mission_summ_processed["PERC_PROMO_UNITS"],
        df_mission_summ_processed["PERC_PROMO_SALES"],
        df_mission_summ_processed["PERC_TOTAL_SALES"],
        df_mission_summ_processed["PERC_TOTAL_UNITS"],
        df_mission_summ_processed["PERC_TOTAL_VISITS"],
    ]

    # perform the join
    df = (
        df_merge_clusters.join(df_store_list_processed, "BANNER_KEY", "left")
        .join(df_mission_summ_processed, "STORE_PHYSICAL_LOCATION_NO", "left")
        .select(*cols)
    )

    # Update for consistency
    df = df.withColumnRenamed("BANNER_ENGINE", "BANNER")
    df = df.withColumnRenamed("REGION_ENGINE", "REGION")

    df = df.withColumn("BANNER", F.upper(F.col("BANNER")))
    df = df.withColumn("REGION", F.upper(F.col("REGION")))

    return df


def log_summary(df: SparkDataFrame) -> None:
    """
    Logs summary for resulting file 1
    % of nulls in each column

    Parameters
    ----------
    df: file 1 dataset

    Returns
    -------
    None
    """
    dims = ["BANNER_KEY", "REGION", "BANNER"]
    dict_cols = {x: x for x in df.columns if x not in dims}

    n_rows = df.count()

    # shortcut
    is_bad = is_col_null_mask

    # get percentage of nulls in each column
    cols = {k: F.when(is_bad(x), 0).otherwise(1) for k, x in dict_cols.items()}
    cols = {k: (F.sum(x) / n_rows) for k, x in cols.items()}
    cols = {k: F.round(x * 100, 1).cast(T.StringType()) for k, x in cols.items()}
    cols = [F.concat(x, F.lit("%")).alias(k) for k, x in cols.items()]

    # produce the summary in a tabular form
    pdf_summary = df.select(*cols).toPandas().T

    msg = f"""
    Here is the summary of how data was merged.
    Here is the % of valid records (higher the better) 
    in each column:
    {pdf_summary}
    """

    log.info(msg)
