from itertools import chain
from typing import Tuple

from inno_utils.loggers import log

from spaceprod.src.clustering.external_clustering.feature_clustering.udf import (
    feature_clustering_pre_proc_udf,
)
from spaceprod.utils.imports import F, SparkDataFrame, T
from spaceprod.utils.names import get_col_names


def convert_mapping_data_prefix(
    df_mapping: SparkDataFrame, col_prefix: str
) -> SparkDataFrame:
    """
    Convert the static file's prefix of feature to denominator mapping to the appropriate
    year prefix of environics data source

    Parameters
    ----------
    df_mapping : SparkDataFrame
        external dataset containing mapping between feature<->denominator
    col_prefix : str
        year prefix of environics dataset

    Returns
    -------
    SparkDataFrame
        mapping dataset with the correct year prefix
    """

    prefix = col_prefix + "_"
    df_mapping = df_mapping.withColumn(
        "FEATURE", F.regexp_replace("FEATURE", "2020_", prefix)
    )
    df_mapping = df_mapping.withColumn(
        "DENOMINATOR", F.regexp_replace("DENOMINATOR", "2020_", prefix)
    )
    return df_mapping


def prepare_udf_input_data(
    df_environ: SparkDataFrame,
    df_mapping: SparkDataFrame,
) -> SparkDataFrame:
    """
    Uses inputs that are external to clustering module in order to
    aggregate them on BANNER/REGION level and produce a dataset that will
    will be used to apply UDF in order to pre-process data for external clustering.
    It does it by aggregating data and combining UDF inputs into arrays of maps
    in spark (i.e. lists of dicts) in order to be able to pass to the UDF

    Parameters
    ----------
    df_environ : SparkDataFrame
        environics data created in upstream module
    df_mapping : SparkDataFrame
        external dataset containing mapping between feature<->denominator

    Returns
    -------
    SparkDataFrame
        data on region/banner level to be used with the pre-processing UDF
    """

    log.info("prepare the data")

    # get column names
    n = get_col_names()

    # define the unit of run for UDF (the dimensionality at which the pre-processing will run)
    dims = [n.F_REGION_DESC, n.F_BANNER]

    # aggregate the raw data
    cols_to_pass = [x for x in df_environ.columns if x not in dims]
    col_json = F.create_map(
        list(chain(*((F.lit(name), F.col(name)) for name in cols_to_pass)))
    )
    agg = [F.collect_list(F.col("INPUT_DATA_JSON")).alias("INPUT_DATA_RAW")]
    df_environ = df_environ.withColumn("INPUT_DATA_JSON", col_json)
    df_agg_raw = df_environ.groupBy(*dims).agg(*agg)

    # aggregate the mapping data
    cols_to_pass = ["FEATURE", "DENOMINATOR"]
    col_json = F.create_map(
        list(chain(*((F.lit(name), F.col(name)) for name in cols_to_pass)))
    )
    agg = [F.collect_list(F.col("INPUT_DATA_JSON")).alias("INPUT_DATA_MAPPING")]
    df_mapping = df_mapping.withColumn("INPUT_DATA_JSON", col_json)
    df_agg_mapping = df_mapping.agg(*agg)

    # created combined input to pre-processing
    df_input = df_agg_raw.crossJoin(df_agg_mapping.select("INPUT_DATA_MAPPING"))

    return df_input


def run_external_clustering_pre_processing(df_input: SparkDataFrame) -> SparkDataFrame:
    """
    prepare the UDF output schema and create running plan for UDF. Note that this function
    does not actually execute the UDF until it materializes

    Parameters
    ----------
    df_input : SparkDataFrame
        input dataframe with parallelization unit as columns and all the data in each parallelization units
        in the shape of map of string to string

    Returns
    -------
    SparkDataFrame
        output of the UDF which is still in the form of tuple of dictionaries added to the input column
    """

    # define column names to contain the whole dataframes as key-value pair
    cols_to_pass = [
        F.col("INPUT_DATA_RAW"),
        F.col("INPUT_DATA_MAPPING"),
    ]

    # define the structure / typing of the return data
    schema_return = T.StructType(
        [
            T.StructField(
                "post_normalized_records",
                T.ArrayType(T.MapType(T.StringType(), T.FloatType())),
            ),
            T.StructField(
                "pre_normalized_records",
                T.ArrayType(T.MapType(T.StringType(), T.FloatType())),
            ),
            T.StructField(
                "corr_list", T.ArrayType(T.MapType(T.StringType(), T.StringType()))
            ),
        ]
    )

    # create udf and apply to the input dataframe
    log.info("process and normalize the data")

    lamb_func = (
        lambda input_data_raw, input_data_mapping: feature_clustering_pre_proc_udf(
            input_data_raw, input_data_mapping
        )
    )
    udf_func = F.udf(lamb_func, schema_return)
    df_udf_output = df_input.withColumn("PRE_PROC_UDF_OUTPUT", udf_func(*cols_to_pass))

    return df_udf_output


def extract_results(
    df_udf_output: SparkDataFrame,
) -> Tuple[SparkDataFrame, SparkDataFrame, SparkDataFrame]:
    """
    This function extracts the column of tuple of dictionaries into a couple of different
    dataframes with proper formatting

    Parameters
    ----------
    df_udf_output : SparkDataFrame
        The output of UDF with all the outputs packed in a tuple of dictionary in a column
    Returns
    -------
    Tuple[SparkDataFrame, SparkDataFrame, SparkDataFrame]
        3 dataframes:
            - df_post_normalized : the environics data after processing with denominator and normalization. Used as clustering input
            - corr_list : correlation w.r.t. to sales per sq ft for each feature
            - df_pre_normalized : the environics data after processing with denominator, but unnormalized. Used for dashboarding
    """

    # get column names
    n = get_col_names()

    # define the unit of run for UDF (the dimensionality at which the pre-processing will run)
    dims = [n.F_REGION_DESC, n.F_BANNER]

    # separate the dataframes into post-normalized, pre-normalized, and corr_list dataframes

    # post-normalized records are environics data after processing it with denominators and normalization
    # it is used as direct input to clustering models
    col = F.col("PRE_PROC_UDF_OUTPUT")["post_normalized_records"].alias(
        "PRE_PROCESSED_DATA"
    )
    df_post_normalized = df_udf_output.select(*dims, col)

    # pre-normalized records are environics data after processing it with denominators without normalization
    # it is used as for dashboards to keep the ratio numbers preserved and unaltered
    col = F.col("PRE_PROC_UDF_OUTPUT")["pre_normalized_records"].alias(
        "PRE_PROCESSED_DATA"
    )
    df_pre_normalized = df_udf_output.select(*dims, col)

    # corr_list is the list of correlations of all features w.r.t. sales per sq ft
    # features within each cluster (as modeled later) with highest correlation is picked as cluster representative
    col = F.col("PRE_PROC_UDF_OUTPUT")["corr_list"].alias("PRE_PROCESSED_DATA")
    df_corr_list = df_udf_output.select(*dims, col)

    # Convert the dictionary output column into properly formatted table
    drop_cols_pre_pivot = ("INPUT_DATA_RAW", "INPUT_DATA_MAPPING", "PRE_PROCESSED_DATA")
    drop_cols_post_pivot = ("exploded_arr", "PRE_PROCESSED_DATA", "id")

    # for the normalized df
    log.info("extract the normalized data from UDF")

    df_post_normalized = df_post_normalized.withColumn(
        "exploded_arr", F.explode("PRE_PROCESSED_DATA")
    )
    df_post_normalized = df_post_normalized.withColumn(
        "id", F.monotonically_increasing_id()
    )
    df_post_normalized = df_post_normalized.select(
        *df_post_normalized.columns, F.explode("exploded_arr")
    )
    df_post_normalized = df_post_normalized.drop(*drop_cols_pre_pivot)
    df_post_normalized = (
        df_post_normalized.groupBy(*dims + ["id"]).pivot("key").agg(F.first("value"))
    )
    df_post_normalized = df_post_normalized.drop(*drop_cols_post_pivot)

    # for the pre-normalized df
    log.info("extract the pre-normalized data from UDF")

    df_pre_normalized = df_pre_normalized.withColumn(
        "exploded_arr", F.explode("PRE_PROCESSED_DATA")
    )
    df_pre_normalized = df_pre_normalized.withColumn(
        "id", F.monotonically_increasing_id()
    )
    df_pre_normalized = df_pre_normalized.select(
        *df_pre_normalized.columns, F.explode("exploded_arr")
    )
    df_pre_normalized = df_pre_normalized.drop(*drop_cols_pre_pivot)
    df_pre_normalized = (
        df_pre_normalized.groupBy(*dims + ["id"]).pivot("key").agg(F.first("value"))
    )
    df_pre_normalized = df_pre_normalized.drop(*drop_cols_post_pivot)

    # convert Banner key to string in both tables as it was exported as float in UDF
    col = F.col(n.F_BANNER_KEY).cast(T.LongType()).cast(T.StringType())
    df_post_normalized = df_post_normalized.withColumn(n.F_BANNER_KEY, col)
    col = F.col(n.F_BANNER_KEY).cast(T.LongType()).cast(T.StringType())
    df_pre_normalized = df_pre_normalized.withColumn(n.F_BANNER_KEY, col)

    # for the corr_list df
    log.info("extract the corr_list from UDF")

    df_corr_list = df_corr_list.withColumn(
        "exploded_arr", F.explode("PRE_PROCESSED_DATA")
    )
    df_corr_list = df_corr_list.withColumn("id", F.monotonically_increasing_id())
    df_corr_list = df_corr_list.select(*df_corr_list.columns, F.explode("exploded_arr"))
    df_corr_list = df_corr_list.drop(*drop_cols_pre_pivot)
    df_corr_list = (
        df_corr_list.groupBy(*dims + ["id"]).pivot("key").agg(F.first("value"))
    )
    df_corr_list = df_corr_list.drop(*drop_cols_post_pivot)

    return df_post_normalized, df_corr_list, df_pre_normalized
