import collections
from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd

from inno_utils.loggers import log
from inno_utils.loggers.log import format_title
from pyspark.sql import Column, SparkSession

from spaceprod.utils.data_helpers import backup_on_blob
from spaceprod.utils.data_transformation import check_invalid, union_dfs
from spaceprod.utils.imports import F, SparkDataFrame, T
from spaceprod.utils.validation import dup_check


def log_item_count(df: SparkDataFrame, msg: str, col_name: str = "ITEM_NO") -> None:
    n_items = df.select(col_name).dropDuplicates().count()
    log.info(f"ITEM COUNT: {n_items} ({msg})")


def limit_modeling_scope(
    spark: SparkSession,
    df_inputs: SparkDataFrame,
    item_limit_threshold: int,
    list_hard_excluded_exec_ids: List[str],
) -> SparkDataFrame:
    """
    Some cases are not eligible for modelling.
    For e.g. with <9 categories

    2 possible reason why we exclude POGs here:

     1. POG data is not comprehensive because some POG Sections are missing
        item information, therefore we get POGs that "look small" (i.e. <9 itmes)
        Work with POG Data provider to see if the data can be enriched.

     2. Model isn't supporting low-item-count POG Sections and locks (freezes).
        Make a decision if we want to refactor model, or just be ok with dropping.

    Parameters
    ----------
    spark: spark session
    df_inputs: input data to modeling UDF
    item_limit_threshold: only include POGs with at least this number of items
    list_hard_excluded_exec_ids: force-exclude Exec IDs from this list

    Returns
    -------
    lists of exec IDs that will be exlcluded for modeling UDF
    """

    mask = F.col("ITEM_COUNT") < item_limit_threshold
    df_threshold = df_inputs.filter(mask).select("EXEC_ID").dropDuplicates()
    df_threshold = df_threshold.withColumn("REASON_THRESHOLD", F.lit(1))
    runs_excluded = [x[0] for x in df_threshold.select("EXEC_ID").collect()]

    msg = f"""
    There were {len(runs_excluded)} Model Exec IDs that were excluded  
    as they had less than {item_limit_threshold} items. Here are they:
    - {runs_excluded}
    """

    log.info(msg)

    msg = f"""
    The following Exec IDs are also excluded based on config inputs:
    {list_hard_excluded_exec_ids}
    """

    log.info(msg)

    schema = T.StructType([T.StructField("EXEC_ID", T.StringType(), True)])
    pdf_to_exclude = pd.DataFrame({"EXEC_ID": list_hard_excluded_exec_ids})
    df_config = spark.createDataFrame(pdf_to_exclude, schema=schema)
    df_config = df_config.withColumn("REASON_CONFIG", F.lit(1))

    # combine together various reasons into separate lists
    df = df_threshold.join(df_config, "EXEC_ID", "full_outer")

    return df


def get_mask_error() -> Column:
    """Returns a mask for finding errors in model output"""

    # for e.g. Exception is available in the first row under "Exception" key
    # for successful there would be no such key in the map
    # while broken ones have a key "Exception" in the first element of list
    col_error = F.col("EMBEDD_NAME")
    mask_error = col_error.startswith("EXCEPTION")
    return mask_error


def validate_prod2vec_output(df: SparkDataFrame) -> None:
    """
    asserts to ensure that we did not get any errors as part of
    prod2vec run using UDF
    """

    # for those model runs that resulted in errors, Traceback and Exception
    # messages are written to the 'model_output' column
    # for e.g. Exception is available in the first row under "Exception" key
    # for successful there would be no such key in the map
    # while broken ones have a key "Exception" in the first element of list
    mask_error = get_mask_error()

    # find broken runs
    # errors are stored in EMBEDD_NAME
    cols = [F.col("EXEC_ID"), F.col("EMBEDD_NAME").alias("ERROR_MSG")]
    df_errors = df.filter(mask_error).select(*cols)
    dup_check(df_errors, ["EXEC_ID"])
    errors = df_errors.collect()
    n_errors = len(errors)
    log.info(f"The distributed training resulted in {n_errors} errors!")

    # log the exact errors
    msg_errors = ""

    # to separate errors
    msg_line = 50 * "-"

    # to create a heading
    msg = f"The following {n_errors} errors occurred during model run:"
    msg_title = format_title(msg)

    # construct the message for logging
    for x in errors:
        exec_id = x["EXEC_ID"]
        msg_error = x["ERROR_MSG"]
        msg_errors += f"\nFor '{exec_id}':\n{msg_error}\n" + msg_line

    msg = f"""
    {msg_title}
    {msg_errors}
    Errors occurred during model run (see above list of tracebacks).
    Model output was still preserved.
    Total number of errors: {n_errors}
    """

    if n_errors > 0:
        raise Exception(msg)


def create_data_for_prod2vec_pairs(
    pdf: pd.DataFrame,
) -> Tuple[
    List[List[List[int]]],
    List[Tuple[Union[str, int], int]],
    Dict[Union[str, int], int],
    Dict[int, Union[str, int]],
]:
    """Function to create counts of items, a dictionary mapping between ITEM_NO and an index,
    a reversed dictionary that maps back index to ITEM_NO and the transaction data with ITEM_NO
    mapped to the index

    Parameters
    ----------
    pdf : pd.DataFrame
        data passed to the UDF
    num_prods : int
        the number of items on which to train the embeddings e.g. top X items
        (all others are tagged as "UNK" (unknown))

    Returns
    -------
    all_basket_data : array
       array of lists containing the index of the product_id purchased in every basket
    count : array
        array of tuples containing the product_id and the count of the number of baskets
        the product is found in
    dictionary : dict
        dictionary containing the mapping of product_id to index
    reversed_dictionary : dict
        dictionary containing the reverse mapping of index to product_id
    """

    item_tuple_list = pdf[["ITEM_A", "ITEM_B"]].values.tolist()

    pdf["ITEM_PAIR"] = item_tuple_list

    customer_item_tuple_list = (
        pdf.groupby("CUSTOMER_CARD_ID")["ITEM_PAIR"].apply(list).to_dict()
    )

    # Create counts of products
    # Placeholder for unknown - it tags items that are not in the most common num_prods as unknown
    count = [["UNK", -1]]

    # creating common (flat) list of items to be able to create indices
    item_tuple_list_flat = [item for tuple in item_tuple_list for item in tuple]

    # .most_common(...) here used to be used to limit the number of
    # observations, but we no longer do it, instead we still use
    # .most_common(...) to convert the collections.Counter object to a list
    # of tuples to ensure compatibility with below logic. We pass the
    # total number of items in the flattened tuple list to ensure all
    # are preserved
    n_items = len(item_tuple_list_flat)
    count.extend(collections.Counter(item_tuple_list_flat).most_common(n_items))

    # Create a dictionary mapping of product to index
    dictionary = dict()
    for prod, _ in count:
        dictionary[prod] = len(dictionary)

    # Create a reversed mapping of index to product
    reversed_dictionary = dict(zip(dictionary.values(), dictionary.keys()))

    # Get counts for unknown products and map the product index from the dictionary
    # to each product in the basket
    # this is done by each item group and within the item group we iterate over all items and count them.

    customer_item_tuple_list_vals = list(customer_item_tuple_list.values())

    unk_count = 0
    all_basket_data = list()
    for i_customer in range(0, len(customer_item_tuple_list_vals)):

        basket_list = list()

        for item_pair in customer_item_tuple_list_vals[i_customer]:
            item_target = item_pair[0]
            item_context = item_pair[1]

            if item_target in dictionary:
                index_target = dictionary[item_target]
            else:
                index_target = 0  # dictionary['UNK']
                unk_count += 1

            if item_context in dictionary:
                index_context = dictionary[item_context]
            else:
                index_context = 0  # dictionary['UNK']
                unk_count += 1

            # index is a tuple of target + context item indices
            index = [index_target, index_context]
            basket_list.append(index)

        all_basket_data.append(basket_list)

    count[0][1] = unk_count

    # converting all types to int
    all_basket_data = [[[int(z) for z in y] for y in x] for x in all_basket_data]

    reversed_dictionary = {int(k): str(v) for k, v in reversed_dictionary.items()}

    return all_basket_data, count, dictionary, reversed_dictionary


def pivot_result(df: SparkDataFrame) -> SparkDataFrame:
    """
    converts POG-level output data that is returned by UDF
    back into granular data
    Parameters
    ----------
    df: POG-level output data that is returned by UDF

    Returns
    -------
    same model output by category level melted into items/embeddings
    """

    # log item count
    mask = F.col("ITEM_NO") != "-1"
    log_item_count(df.filter(mask), "after prod2vec model run")

    # first exclude errors (they will only exist at this point if the
    # user decided to proceed despite the assertion errors in previous step)

    mask_error = get_mask_error()
    df = df.filter(~mask_error)

    dup_check(df, ["EXEC_ID", "ITEM_NO", "EMBEDD_NAME"])

    dims = ["EXEC_ID", "ITEM_NO", "index"]
    agg = [F.first("EMBEDD_VALUE")]
    df_embed_name = df.select("EMBEDD_NAME").dropDuplicates()
    cols_embed = sorted([x[0] for x in df_embed_name.collect()])
    df_result_melt = df.groupBy(*dims).pivot("EMBEDD_NAME", values=cols_embed).agg(*agg)

    # log item count
    mask = F.col("ITEM_NO") != "-1"
    log_item_count(df_result_melt.filter(mask), "after melting model results")

    return df_result_melt


def patch_embeddings_data(df: SparkDataFrame) -> SparkDataFrame:
    """
    Patches column types and adds back 'UNK' value
    """

    # convert ITEM_NO type
    col = F.col("ITEM_NO").cast(T.LongType()).cast(T.StringType())
    df = df.withColumn("ITEM_NO", col)

    # add back 'UNK' value
    mask = F.col("ITEM_NO") == "-1"
    col = F.when(mask, F.lit("UNK")).otherwise(F.col("ITEM_NO"))
    df = df.withColumn("ITEM_NO", col)

    # log item count
    mask = F.col("ITEM_NO") != "UNK"
    log_item_count(df.filter(mask), "after patching model results")

    return df


def generate_execution_id(df: SparkDataFrame) -> SparkDataFrame:
    """
    generate data dimensions <-> execution ID mapping
    The execution ID is a unique combination of dimensions.
    Each ID represents a single model run

    Parameters
    ----------
    df: pre-processed data for prod2vec

    Returns
    -------
    same data with execution ID
    """
    dims = ["REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER"]
    cols = sum([[F.col(i), F.lit("_")] for i in dims], [])[:-1]
    col = F.regexp_replace(F.concat(*cols), "[^0-9A-Za-z_]+", "")
    df = df.withColumn("EXEC_ID", col)

    return df


def validate_execution_id(
    df: SparkDataFrame,
    col_name_exec_id: Optional[str] = "EXEC_ID",
) -> None:
    """
    ensures that the EXEC_ID (column in dataframe) is not shared by more
    than 1 region/banner/section master
    """

    # if we drop dups for dimensional columns + EXEC_ID, the resulting
    # dataset MUST be on on EXEC_ID level if the EXEC_ID was mapped correctly
    # previously. lets check for this

    col_exec_id = F.col("EXEC_ID").alias(col_name_exec_id)

    cols_dims = [
        "REGION",
        "NATIONAL_BANNER_DESC",
        "SECTION_MASTER",
    ]

    cols_dims_all = [col_exec_id] + cols_dims

    df_lookup = df.select(*cols_dims_all).dropDuplicates()
    dup_check(df_lookup, [col_name_exec_id])


def lookup_region_banner_pog(
    df: SparkDataFrame,
    df_cust_item_diff: SparkDataFrame,
    col_name_exec_id: Optional[str] = "EXEC_ID",
) -> SparkDataFrame:
    """
    Re-adds the dimensional columns based on EXEC_ID.

    Parameters
    ----------
    df: a dataset containing EXEC_ID
    df_cust_item_diff: a dataset containing EXEC_ID and dimensional columns
    col_name_exec_id: custom EXEC_ID column name in the 'df' table

    Returns
    -------
    original 'df' with dimensional columns
    """

    validate_execution_id(df_cust_item_diff, "EXEC_ID")

    col_exec_id = F.col("EXEC_ID").alias(col_name_exec_id)

    cols_dims = [
        "REGION",
        "NATIONAL_BANNER_DESC",
        "SECTION_MASTER",
    ]

    cols_dims_all = [col_exec_id] + cols_dims

    df_lookup = df_cust_item_diff.select(*cols_dims_all).dropDuplicates()

    # perform the lookup
    df = df.join(df_lookup, col_name_exec_id, "left")

    # final check that we got 100% matches
    cols_check = [col_name_exec_id] + cols_dims
    check_invalid(df, cols_check)

    return df


def pre_process_data_for_prod2vec(
    df_cust_item_diff: SparkDataFrame,
) -> SparkDataFrame:
    """
    transforms data by adding necessary columns and doing transformations
    to prepare data for feeding to the prod2vec UDF
    Parameters
    ----------
    df_cust_item_diff: SparkDataFrame
        item pairs data by customer with EXEC_ID

    Returns
    -------
    processed data
    """

    df = df_cust_item_diff

    # filter out "unknown_section_master"
    # ensures we don't run for items for which we don't have SECTION_MASTER
    mask = F.col("SECTION_MASTER") != "unknown_section_master"
    df = df.filter(mask)

    # calc item counts
    agg = [F.countDistinct("ITEM_A").alias("ITEM_COUNT")]
    df_counts = df.groupBy("EXEC_ID").agg(*agg)
    df = df.join(df_counts, "EXEC_ID", "left")

    # get rid of redundant pairs (A/B and B/A)
    col_array = F.sort_array(F.array(F.col("ITEM_A"), F.col("ITEM_B"))).alias(
        "PAIR_LIST"
    )
    dims = ["EXEC_ID", "CUSTOMER_CARD_ID", "PAIR_LIST"]
    df = df.select(F.col("*"), col_array).dropDuplicates(subset=dims).drop("PAIR_LIST")

    return df


def log_pairs_summary(df_cust_item_diff: SparkDataFrame) -> None:
    """
    Spits in logs a summary of execution IDs generated.
    Show largest 15 by item count.
    Useful to understand what will be the biggest model run

    Parameters
    ----------
    df_cust_item_diff: SparkDataFrame
        item pairs data by customer with EXEC_ID

    Returns
    -------
    None
    """
    df = df_cust_item_diff

    agg = [
        F.count("*").alias("count"),
        F.countDistinct("ITEM_A").alias("items"),
        F.countDistinct("ITEM_A", "ITEM_B").alias("pairs"),
    ]

    df_summary = df.groupBy("EXEC_ID").agg(*agg).orderBy(F.col("count").desc())

    # get the largest 15 execution IDs for spitting to logs
    pdf_summary_15 = df_summary.limit(15).toPandas()

    log.info(f"Top 15 Exec IDs:\n{pdf_summary_15.to_string()}\n")


def call_prod2vec_model_udf(
    spark: SparkSession, df_prod2vec_staged_data: SparkDataFrame, udf: Callable
):
    """
    Calls the udf for prod2vec model to execute the model train in parallel
    as part of a spark UDF.
    Each model run happens within EXEC_ID.

    Parameters
    ----------
    spark: SparkSession
        Spark session instantiated with the pipeline run
    df_prod2vec_staged_data: SparkDataFrame
        data pre-processed upstream, will be used to
        get the list of of EXEC_ID to run for

    udf: Callable
        a pandas UDF function generated upstream

    Returns
    -------
    model output for all EXEC_ID's
    """

    # this skeleton dataset is a small dataset with single column: EXEC_ID
    # it is needed to call the UDF because the only peice of information
    # we are feeding to the UDF is the EXEC_ID string which will be used
    # by UDF to locate the right partition to read data from.
    # Note: we are not passing data to the UDF because there is an internal
    # limitation of how much data can be handled by PyArrow. More information
    # here:
    #  1) https://www.mail-archive.com/search?l=issues@spark.apache.org&q=subject:%22%5C%5Bjira%5C%5D+%5C%5BCommented%5C%5D+%5C%28SPARK%5C-33576%5C%29+PythonException%5C%3A+An+exception+was+thrown+from+a+UDF%5C%3A+%27OSError%5C%3A+Invalid+IPC+message%5C%3A+negative+bodyLength%27.%22&o=newest&f=1
    #  2) https://issues.apache.org/jira/browse/SPARK-33576?page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel=17248064#comment-17248064
    # If we pass more data to PyArrow than it can handle. It will throw the
    # the following exception: 'OSError: Invalid IPC message: negative bodyLength'
    df_skeleton = df_prod2vec_staged_data.select("EXEC_ID").dropDuplicates()
    df_skeleton = backup_on_blob(spark, df_skeleton)
    df_skeleton = df_skeleton.repartition("EXEC_ID")
    n_runs = df_skeleton.count()
    log.info(f"Running model for {n_runs} runs")

    # calls the UDF
    df_result = df_skeleton.groupby("EXEC_ID").apply(udf)

    return df_result


def select_staged_data_columns_and_rows(
    df_prod2vec_limit: SparkDataFrame,
    df_exclusions: SparkDataFrame,
) -> SparkDataFrame:
    """
    In order to limit how much data is being consumed by UDF, we select
    only required columns
    """

    cols = ["EXEC_ID", "CUSTOMER_CARD_ID", "ITEM_A", "ITEM_B"]
    df = df_prod2vec_limit.select(*cols)

    # validate exclusions - make sure there is no EXEC IDs without reasons
    cols = [x for x in df_exclusions.columns if x.startswith("REASON_")]
    check_invalid(df_exclusions, cols, False)

    # exclude filtered out exec IDs
    df_exclusions = df_exclusions.select("EXEC_ID").dropDuplicates()
    df = df.join(df_exclusions, "EXEC_ID", "left_anti")

    return df


def create_exec_id_run_dimensions_lookup(
    df: SparkDataFrame,
    col_name_exec_id: Optional[str] = "EXEC_ID",
) -> SparkDataFrame:
    """
    creates a EXEC_ID <-> run dimensions lookup matrix for use downstream

    Parameters
    ----------
    df: a dataset containing EXEC_ID
    col_name_exec_id: custom EXEC_ID column name in the 'df' table

    Returns
    -------
    original 'df' with dimensional columns
    """

    validate_execution_id(df, "EXEC_ID")

    col_exec_id = F.col("EXEC_ID").alias(col_name_exec_id)

    cols_dims = [
        "REGION",
        "NATIONAL_BANNER_DESC",
        "SECTION_MASTER",
    ]

    cols_dims_all = [col_exec_id] + cols_dims

    df_lookup = df.select(*cols_dims_all).dropDuplicates()

    dup_check(df_lookup, [col_name_exec_id])

    return df_lookup


def log_entity_counts_staged_pairs(
    df_prod2vec_selected: SparkDataFrame,
    df_exec_id_run_dimensions_lookup: SparkDataFrame,
) -> None:
    """
    Logs entity counts in the final processed data.
    This version of function is used to count entities when the staged
    data is using pairs
    Useful to learn how many entities were dropped.

    Parameters
    ----------
    df_prod2vec_selected : SparkDataFrame
        data staged for prod2vec modelling that is using pairs of items

    df_exec_id_run_dimensions_lookup : SparkDataFrame
         the lookup for region/banner/section_name dimension back from
         exec_id

    Returns
    -------
    None
    """

    # shortcut
    df = df_prod2vec_selected

    # calculate straight counts
    n_model_runs = df.select("EXEC_ID").dropDuplicates().count()
    # getting combined item count between ITEM_A and ITEM_B
    n_items = (
        union_dfs(
            [
                df.select(F.col("ITEM_A").alias("i")).dropDuplicates(),
                df.select(F.col("ITEM_B").alias("i")).dropDuplicates(),
            ],
            align_schemas=True,
        )
        .select("i")
        .dropDuplicates()
        .count()
    )

    # generate a summary to print in logs
    msg = f"""
    Task finished running, here is a count summary:
    - Model Runs (EXEC_ID): {n_model_runs}
    - Items (ITEM_A & ITEM_B): {n_items}
    """

    log.info(msg)

    agg = [
        F.count("*").alias("ROW_COUNT"),
        F.countDistinct("SECTION_MASTER").alias("POG_COUNT"),
        F.countDistinct("ITEM_A").alias("ITEM_COUNT_A"),
        F.countDistinct("ITEM_B").alias("ITEM_COUNT_B"),
    ]

    dup_check(df_exec_id_run_dimensions_lookup, ["EXEC_ID"])
    df = df.join(df_exec_id_run_dimensions_lookup, "EXEC_ID", "left")
    pdf = df.groupBy("REGION", "NATIONAL_BANNER_DESC").agg(*agg).toPandas()
    pdf = pdf.sort_values("ROW_COUNT", ascending=False).reset_index(drop=True)

    pd.set_option("display.max_rows", None)

    msg = f"""
    Here is the summary of which region/banner combinations were produced
    as well as their corresponding counts of POGs (SECTION_MASTER)
    as well as items (ITEM_A & ITEM_B)
    sorted by row count from largest to lowest.
    \n{pdf}
    """
    log.info(msg)


def log_entity_counts_prod2vec(df_prod2vec: SparkDataFrame) -> None:
    """logs entity counts in the final processed data"""

    # shortcut
    df = df_prod2vec

    # calculate straight counts
    n_model_runs = df.select("EXEC_ID").dropDuplicates().count()
    n_items = df.select("ITEM_NO").dropDuplicates().count()

    # generate a summary to print in logs
    msg = f"""
    Task finished running, here is a count summary:
    - Model Runs (EXEC_ID): {n_model_runs}
    - Items (ITEM_NO): {n_items}
    """

    log.info(msg)

    agg = [
        F.count("*").alias("ROW_COUNT"),
        F.countDistinct("SECTION_MASTER").alias("POG_COUNT"),
        F.countDistinct("ITEM_NO").alias("ITEM_COUNT"),
    ]

    pdf = df.groupBy("REGION", "NATIONAL_BANNER_DESC").agg(*agg).toPandas()
    pdf = pdf.sort_values("ROW_COUNT", ascending=False).reset_index(drop=True)

    pd.set_option("display.max_rows", None)

    msg = f"""
    Here is the summary of which region/banner combinations were produced
    as well as their corresponding counts of POGs (SECTION_MASTER)
    as well as items (ITEM_NO)
    sorted by row count from largest to lowest.
    \n{pdf}
    """
    log.info(msg)


class TimeOutForModel:
    """timeout only works on non-windows at the moment"""

    def __init__(self, timeout_min: int):
        if not self._is_available():
            log.info(f"WARNING: TIME OUT NOT IN FORCE FOR YOUR SYSTEM!")
            return

        import signal

        self._signal = signal

        def timeout_handler(a, b):
            raise Exception(f"Model train timeout reached! ({timeout_min} minutes)")

        self._signal.signal(signal.SIGALRM, timeout_handler)
        self._signal.alarm(timeout_min * 60)

    def cancel(self):
        if not self._is_available():
            log.info(f"WARNING: TIME OUT NOT IN FORCE FOR YOUR SYSTEM!")
            return

        self._signal.alarm(0)

    def _is_available(self):
        import platform

        return platform.system().lower() != "windows"
