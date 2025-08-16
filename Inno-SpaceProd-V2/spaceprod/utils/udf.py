from typing import List, Callable

from pyspark import Row
from pyspark.sql import SparkSession, Column as SparkColumn

from spaceprod.utils.imports import SparkDataFrame


def run_udf_locally_sequentially(
    spark: SparkSession,
    df_input: SparkDataFrame,
    result_var_name: str,
    cols_to_pass: List[str],
    udf_function: Callable,
) -> SparkDataFrame:
    """
    This function is not actully running a Spark UDF in a normal way,
    instead it loops a callable provided by 'udf_function' for each row
    in the spark df ('df_input') and stores result in 'result_var_name' as
    a new column

    NOTE: SHOULD NOT BE USED IN A PIPELINE FOR LARGE-SCALE RUNS
    THIS IS FOR TESTING/DEBUGGING PURPOSES ONLY

    Parameters
    ----------
    spark: spark session generated for this run
    df_input: any input spark dataframe
    result_var_name: name of column where to store results
    cols_to_pass: which columns to be passed to 'udf_function' (positional)
    udf_function: a callable function to be called on each row of 'df_input'

    Returns
    -------
    spark DF from 'df_input' but with 'result_var_name' column
    """

    coll_df = df_input.select(*cols_to_pass).collect()

    list_results = []
    for r in coll_df:
        result = udf_function(*r.asDict().values())
        result_row = r.asDict()
        result_row[result_var_name] = result
        list_results.append(Row(**result_row))

    df_result = spark.createDataFrame(list_results)

    return df_result


def set_shuffle_partitions_prior_to_udf_run(
    df_skeleton: SparkColumn,
    dimensions: List[str],
) -> None:
    """
    ensures that udf (opt) runs distributed in the most
    efficient way - 1 udf run per worker/core combination
    Must be called prior to applying the UDF function

    Parameters
    ----------
    df_skeleton : SparkDataFrame
        the skeleton dataframe that will be used to run the UDF on

    dimensions : List[str]
        set of columns of 'df_skeleton' that represent a single slice
        of data that will be sent to each individual UDF run

    Returns
    -------
    None
    """
    from spaceprod.utils.space_context.spark import spark

    n = df_skeleton.select(*dimensions).dropDuplicates().count()
    spark.conf.set("spark.sql.shuffle.partitions", str(n))
