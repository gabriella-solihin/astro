import datetime as dt
import hashlib
import os
import pickle
import re
import shutil
import time
import traceback
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union, Callable, Any
from uuid import uuid4
from functools import reduce
import pandas as pd

from inno_utils.loggers import log
from inno_utils.azure import get_files_in_blob_folder
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

# global variable defining the location where  test integration runs paths
# would live
from spaceprod.utils import (
    PATH_TEMP_CACHE_SCHEMA,
    PATH_DOWNLOADS_LOCAL,
    ROOT_PATH_BLOB_SCHEMA,
    PATH_TEMP_LOCAL,
    ROOT_PATH_BLOB_LOCAL,
)
from spaceprod.utils.data_transformation import union_dfs
from spaceprod.utils.imports import F


def strip_except_alpha_num(s, replace_string="_"):
    """Remove all non-word characters (everything except numbers and letters)
    TODO: decommission this function

    Parameters
    ----------
    s: str
    replace_string: str
    Returns
    ----------
    s: str
    """
    s = s.strip()
    s = re.sub(r"[^\w\s]", "", s)
    # Replace all runs of whitespace with a single dash
    s = re.sub(r"\s+", replace_string, s)
    return s.title()


def strip_except_alpha_num_in_pdf_columns(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Applies 'strip_except_alpha_num' to every column name in a given Pandas
    dataframe to rename the fields to title case.


    Parameters
    ----------
    pdf : pd.DataFrame
        pdf to be rename

    Returns
    -------
    renamed pdf
    """
    pdf_result = pdf.rename(
        columns=dict(
            zip(
                pdf.columns,
                [strip_except_alpha_num(x) for x in pdf.columns],
            )
        ),
        inplace=False,
    )

    return pdf_result


def snake_case(s: str, replace_string="_") -> str:
    """This function rewrites strings into snake case. It's used mostly for column names to have a uniform convention

    Parameters
    ----------
    s: str
    replace_string: str
    Returns
    ----------
    s: str
    """
    if not isinstance(s, str):
        s = str(s)
    # Remove all non-word characters (everything except numbers and letters)
    s = s.strip()
    s = re.sub(r"[^\w\s]", "", s)
    # Replace all runs of whitespace with a single dash
    s = re.sub(r"\s+", replace_string, s)
    return s


def read_delta_table(spark: SparkSession, path: str, table_name: str) -> SparkDataFrame:
    """this function reads a delta table

    Parameters
    ----------
    spark : spark session
    path: str
        path of table to read
    table_name: str
        name of the table
    Returns
    ---------
    delta_table: SparkDataFrame
        filtered dataframe

    """

    log.info(f"Reading: '{path}'")

    delta_table = spark.read.format("delta").load(os.path.join(path, table_name))
    return delta_table


def filter_df_for_container(
    df: pd.DataFrame,
    df_name: str,
    filter_col: str,
    filter_value: Union[str, int, float, bool],
    store=None,
):
    """this function filters for a specific value in a column and then asserts that the resulting dataframe is not empty

    Parameters
    ----------
    df : pd.DataFrame
        dataframe to filter
    df_name: str
        name of dataframe to output in logger info
    filter_col: str
        column to filter on
    filter_value: object
        value to filter on

    Returns
    ---------
    df_output: pd.DataFrame
        filtered dataframe

    """

    df_output = df[df[filter_col] == filter_value]

    # assert len(df_output) > 0, f"{df_name} is empty for {filter_col} = {filter_value}"

    return df_output


def filter_and_assert_not_empty(
    df: pd.DataFrame,
    df_name: str,
    filter_col: str,
    filter_value: Union[str, int, float, bool],
    store=None,
):
    """this function filters for a specific value in a column and then asserts that the resulting dataframe is not empty

    Parameters
    ----------
    df : pd.DataFrame
        dataframe to filter
    df_name: str
        name of dataframe to output in logger info
    filter_col: str
        column to filter on
    filter_value: object
        value to filter on

    Returns
    ---------
    df_output: pd.DataFrame
        filtered dataframe

    """

    df_output = df[df[filter_col] == filter_value]

    assert len(df_output) > 0, f"{df_name} is empty for {filter_col} = {filter_value}"

    return df_output


def write_pickles(path, file, prefix=None, local=False):
    """Function to write pickles to blob

    Parameters
    ----------
    path: str
        path to read from
    file: SparkDataFrame
        object to write
    prefix: str
        needed prefix
    local: bool
        read local or form blob

    """

    """Function to write pickles to blob"""

    log.info(f"Writing: '{path}'")

    if local:
        with open(path, "wb") as f:
            pickle.dump(file, f)
    else:

        # since we are usin Azure utils to download/upload from blob
        # here we need relative blob path only (no dbfs:/mnt/blob)
        path = fix_path_compatability(path_schema=path, full_path=False)

        tmp_file_name = _create_temp_local_path(prefix=prefix, extension="pkl")

        def write_handler(object: Any, path_local: str):
            with open(path_local, "wb") as f:
                pickle.dump(object, f)

        _write_and_upload_an_object(
            object=file,
            path_remote=path,
            path_local=tmp_file_name,
            write_handler=write_handler,
        )


def write_pickles_gz(path, file, prefix=None, local=False):
    """Function to write pickles to blob

    Parameters
    ----------
    path: str
        path to read from
    file: SparkDataFrame
        object to write
    prefix: str
        needed prefix
    local: bool
        read local or form blob

    """

    """Function to write pickles to blob"""

    log.info(f"Writing (pickle gzip): '{path}'")

    import gzip

    if local:
        with gzip.open(path, "wb") as f:
            pickle.dump(file, f)
    else:
        if not prefix:
            prefix = str(uuid4()) + dt.datetime.now().strftime("%Y%m%d%H%M%S%f")

        tmp_file_name = f"/tmp/{prefix}.pkl"

        with gzip.open(tmp_file_name, "wb") as f:
            pickle.dump(file, f)

        from inno_utils.azure import upload_to_blob

        upload_to_blob(path, tmp_file_name)


def remove_local_path(path):
    if not os.path.exists(path):
        log.info(f"No need to remove local path, does not exist: {path}")
        return

    try:
        shutil.rmtree(path)
        log.info(f"Removed using shutil.rmtree: {path}")
    except Exception:
        log.info(f"Did not remove using shutil.rmtree: {path}")

    if not os.path.exists(path):
        return

    try:
        os.unlink(path)
        log.info(f"Removed using os.unlink: {path}")
    except Exception:
        log.info(f"Did not remove using os.unlink: {path}")

    if os.path.exists(path):
        raise Exception(f"Could not remove local path: {path}")


def _download_and_read_a_file(
    path_remote: str,
    path_local: str,
    read_handler: Callable,
):
    from inno_utils.azure import download_from_blob

    try:
        log.info(f"Downloading from {path_remote} to {path_local}")
        download_from_blob(path_remote, path_local)

        data = read_handler(path_local)

    except Exception:
        tb = traceback.format_exc()
        msg = f"Could not download and read file:\n{tb}\n"
        remove_local_path(path_local)
        raise Exception(msg)
    finally:
        remove_local_path(path_local)

    return data


def _write_and_upload_an_object(
    object: Any, path_remote: str, path_local: str, write_handler: Callable
):
    try:
        write_handler(object, path_local)

        from inno_utils.azure import upload_to_blob

        upload_to_blob(path_remote, path_local)
    except Exception:
        tb = traceback.format_exc()
        msg = f"Could not write and upload a file:\n{tb}\n"
        remove_local_path(path_local)
        raise Exception(msg)
    finally:
        remove_local_path(path_local)


def _create_temp_local_path(
    prefix: Optional[str] = None, extension: Optional[str] = None
):
    rand = str(uuid4()) + dt.datetime.now().strftime("%Y%m%d%H%M%S%f")

    if prefix is None:
        temp_file_name = rand
    else:
        temp_file_name = f"{prefix}_{rand}"

    tmp_file_name = f"{PATH_TEMP_LOCAL}/{temp_file_name}"

    if extension is not None:
        tmp_file_name = tmp_file_name + "." + extension

    return tmp_file_name


def read_pickles(path: str, prefix=None, local=False):
    """Function to read pickles from blob

    Parameters
    ----------
    path: str
        path to read from
    prefix: str
        needed prefix
    local: bool
        read local or form blob

    Returns
    ----------
    data: object - could be SparkDataFrame depending on data
    """

    log.info(f"Reading: '{path}'")

    if local:
        with open(path, "rb") as f:
            data = pickle.load(f)
    else:

        # since we are usin Azure utils to download/upload from blob
        # here we need relative blob path only (no dbfs:/mnt/blob)
        path = fix_path_compatability(path_schema=path, full_path=False)

        tmp_file_name = _create_temp_local_path(prefix=prefix, extension="pkl")

        def read_handler(path_local: str):
            with open(path_local, "rb") as f:
                return pickle.load(f)

        data = _download_and_read_a_file(
            path_remote=path,
            path_local=tmp_file_name,
            read_handler=read_handler,
        )

    return data


def read_pickles_gz(path: str, prefix=None, local=False):
    """Function to read pickles from blob

    Parameters
    ----------
    path: str
        path to read from
    prefix: str
        needed prefix
    local: bool
        read local or form blob

    Returns
    ----------
    data: object - could be SparkDataFrame depending on data
    """

    log.info(f"Reading: '{path}'")

    import gzip

    if local:
        with gzip.open(path, "rb") as f:
            data = pickle.load(f)
    else:

        from inno_utils.azure import download_from_blob

        tmp_file_name = _create_temp_local_path(prefix=prefix, extension="pkl")

        download_from_blob(path, tmp_file_name)

        def read_handler(path_local: str):
            with gzip.open(path_local, "rb") as f:
                return pickle.load(f)

        data = _download_and_read_a_file(
            path_remote=path,
            path_local=tmp_file_name,
            read_handler=read_handler,
        )

    return data


def get_paths(path: str) -> Tuple[str, str]:
    """
    Converts DBFS path into local and shema path.
    Often we can use either one or the other depending on the use casee.
    Local path starts with "/dbfs", and schema path starts with "dbfs:/"
    Parameters
    ----------
    path:
        A dbfs path

    Returns a tuple of schema and local path
    -------

    """

    path = str(Path(path).as_posix())

    if not path.startswith(
        (
            "dbfs:/",
            "/dbfs/",
        )
    ):
        return path, path

    if path.startswith("/dbfs/"):
        path_schema = "dbfs:" + path[5:]
        path_local = path
    elif path.startswith("dbfs:/"):
        path_schema = path
        path_local = "/dbfs" + path[5:]
    else:
        raise Exception(f"invalid path: {path}")

    return path_schema, path_local


def get_full_csv_path(system_file_csv_path_on_blob: str) -> str:
    from spaceprod.utils.dbutils import get_dbutils

    path = system_file_csv_path_on_blob
    db = get_dbutils()
    paths = db.fs.ls(path)
    paths_csv = [x for x in paths if x.path.endswith(".csv")]
    assert paths_csv, f"No csv files found in folder: {path}"
    assert len(paths_csv) == 1, f"More than 1 csv found in folder: {path}"
    path_csv = get_paths(paths_csv[0].path)[1]
    return path_csv


def fix_path_compatability(path_schema: str, full_path: Optional[bool] = True):
    """a temporary fix to ensure paths are compatable with internal IO layer"""

    if path_schema.startswith(ROOT_PATH_BLOB_SCHEMA):
        path_schema = path_schema

    elif path_schema.startswith(ROOT_PATH_BLOB_LOCAL):
        path_schema = get_paths(path_schema)[0]

    else:
        path_schema = f"{ROOT_PATH_BLOB_SCHEMA}/{path_schema}"

    if not full_path:
        path_schema = path_schema[(len(ROOT_PATH_BLOB_SCHEMA) + 1) :]

    return path_schema


def exists_blob(spark: SparkSession, path: str, file_format: str = "") -> bool:
    """
    Checks if dataset exists and can be read by attempting to read it.
    Parameters
    ----------
    spark: spark instance
    path: dbfs path

    Returns
    -------
    True if dataset exists and can be read, False otherwise
    """

    path = get_paths(path)[0]

    # TODO: a workaround
    if path.endswith(".csv") or file_format == "csv":
        func = spark.read.csv
        kwargs = {"path": path, "header": True}
    elif file_format == "delta":
        func = read_blob
        kwargs = {"spark": spark, "path": path, "file_format": "delta"}
    else:
        func = read_blob
        kwargs = {"spark": spark, "path": path}

    try:
        func(**kwargs)
        return True
    except Exception:
        return False


def check_if_io_to_test_folder(path: str) -> None:
    """
    checks if the current path is located in the test folder
    This is used to ensure that all tests are only reading (writing) from (to)
    the test location
    """

    path_schema, _ = get_paths(path)
    is_path_test = "/test_space_run_" in path_schema
    is_test = os.getenv("SPACE_IS_INTEG_TEST") is not None

    if is_test and (not is_path_test):
        msg = f"""
        Looks like you are running integ test, but you are not  
        reading (writing) from (to) the test location.
         - Test location must have a run id that starts with "test_"
         - Path being written to (read from): {path_schema}
        """

        raise Exception(msg)


def _write_blob_internal(
    df: SparkDataFrame,
    path_schema: str,
    writing_mode: str,
    partition_by: List[str],
):
    if partition_by:
        (
            df.repartition(*partition_by)
            .write.mode(writing_mode)
            .parquet(path_schema, partitionBy=partition_by)
        )

    else:
        df.write.mode(writing_mode).parquet(path_schema)


def _write_blob_csv_internal(
    df: SparkDataFrame,
    path_schema: str,
    encoding: str,
    compression: str,
):
    df.coalesce(1).write.mode("overwrite").csv(
        path=path_schema,
        encoding=encoding,
        compression=compression,
        header=True,
    )


def read_blob_single_path(
    spark: SparkSession,
    path: str,
    **kwargs,
):
    """
    A wrapper around reading from blob.
    This function might become more complex as our data pipeline matures.

    Parameters
    ----------
    spark: spark instance
    path: dbfs path

    Returns
    -------
    a spark dataframe
    """
    # convert path to schema version (if needed)
    path_schema, _ = get_paths(path)

    # TODO: this is a compatibility fix to be able to take paths without root
    path_schema = fix_path_compatability(path_schema)

    # some validation we are not about to write where we are not allowed
    check_if_io_to_test_folder(path_schema)

    log.info(f"Reading: '{path_schema}'")

    df = spark.read.parquet(path_schema, **kwargs)

    return df


def read_blob_list_paths(
    spark: SparkSession,
    list_paths: List[str],
    **kwargs,
) -> SparkDataFrame:
    log.info(f"Reading data from list of paths: {list_paths}")

    # a shortcut
    def r(path: str) -> SparkDataFrame:
        return read_blob_single_path(spark, path, **kwargs)

    l_dfs = [r(x) for x in list_paths]
    df = union_dfs(l_dfs, align_schemas=True)

    return df


def read_blob_dict_paths(
    spark: SparkSession,
    dict_paths: Dict[str, str],
    col_name: str,
    filter_keys: Optional[List[str]] = None,
    **kwargs,
) -> SparkDataFrame:
    if filter_keys is not None:
        dict_paths = {k: v for k, v in dict_paths.items() if k in filter_keys}

    log.info(f"Reading data configured using dict ({col_name}): {dict_paths}")

    # a shortcut
    def r(path: str) -> SparkDataFrame:
        return read_blob_single_path(spark, path, **kwargs)

    def add_col(df: SparkDataFrame, col_name: str, val: str) -> SparkDataFrame:
        msg = f"Make sure none of these DFs have column '{col_name}': {dict_paths}"
        assert col_name not in df.columns, msg
        return df.withColumn(col_name, F.lit(val))

    l_dfs = [add_col(r(v), col_name, k) for k, v in dict_paths.items()]
    df = union_dfs(l_dfs, align_schemas=True)
    return df


def read_blob(
    spark: SparkSession,
    path: Union[str, List[str], Dict[str, str]],
    **kwargs,
) -> SparkDataFrame:
    """
    A wrapper around reading from blob.
    This function might become more complex as our data pipeline matures.

    Parameters
    ----------
    spark: spark instance
    path: dbfs path

    Returns
    -------
    a spark dataframe
    """

    df = read_blob_single_path(
        spark=spark,
        path=path,
        **kwargs,
    )

    return df


def read_blob_txt_files(
    spark: SparkSession,
    path: str,
    **kwargs,
):
    """
    A wrapper around reading from blob (CSV format).
    This function might become more complex as our data pipeline matures.

    Parameters
    ----------
    spark: spark instance
    path: dbfs path

    Returns
    -------
    a spark dataframe
    """
    df_list = []
    files = get_files_in_blob_folder(path)
    # convert path to schema version (if needed)
    for file_path in files:
        path_schema, _ = get_paths(file_path)

        # TODO: this is a compatibility fix to be able to take paths without root
        path_schema = fix_path_compatability(path_schema)

        # some validation we are not about to write where we are not allowed
        check_if_io_to_test_folder(path_schema)

        log.info(f"Reading (txt): '{path_schema}'")

        df_ = (
            spark.read.option("header", "true")
            .option("delimiter", "|")
            .option("inferSchema", "true")
            .csv(path_schema)
        )

        df_list.append(df_)

    df = reduce(SparkDataFrame.unionAll, df_list)

    return df


def read_blob_csv(
    spark: SparkSession,
    path: str,
    **kwargs,
):
    """
    A wrapper around reading from blob (CSV format).
    This function might become more complex as our data pipeline matures.

    Parameters
    ----------
    spark: spark instance
    path: dbfs path

    Returns
    -------
    a spark dataframe
    """

    # convert path to schema version (if needed)
    path_schema, _ = get_paths(path)

    # TODO: this is a compatibility fix to be able to take paths without root
    path_schema = fix_path_compatability(path_schema)

    # some validation we are not about to write where we are not allowed
    check_if_io_to_test_folder(path_schema)

    log.info(f"Reading (CSV): '{path_schema}'")

    df = spark.read.csv(path=path_schema, header=True, **kwargs)

    return df


def write_blob(
    spark: SparkSession,
    df: SparkDataFrame,
    path: str,
    partition_by: Optional[Union[str, List[str]]] = None,
    allow_empty: Optional[bool] = False,
    append: Optional[bool] = False,
) -> None:
    """
    A wrapper around writing to blob in parquet format
    This function might  become more complex as our data pipeline matures.

    Parameters
    ----------
    spark: spark session from pyspark.sql

    df: a spark dataframe to write data from

    path: a local or DBFS path

    partition_by: optionally partition data by this column (currently supports 1 col)


    allow_empty: if False, will break if an empty dataset was written.
        Note: in case of overwrite the previous dataset will be removed

    append: will append data if True, otherwise will replace the data


    Returns
    -------
    """

    # convert path to schema version (if needed)
    path_schema, _ = get_paths(path)

    # TODO: this is a compatibility fix to be able to take paths without root
    path_schema = fix_path_compatability(path_schema)

    # some validation we are not about to write where we are not allowed
    check_if_io_to_test_folder(path_schema)

    # this is a quick and dirty way to safeguard against accidentally writing
    # to prod_outputs, which we should never write to as part of Perso at this
    # stage
    msg = "Looks like you are writing to prod_outputs! This is not allowed!"
    assert "prod_outputs" not in Path(path_schema).parts, msg

    # similar quick and dirty step as above except for accessing PROD paths
    # from DEV
    msg = "Looks like you are writing to PROD from DEV! This is not allowed!"
    assert "mnt/blob_prod" not in path_schema, msg

    writing_mode = "append" if append else "overwrite"

    log.info(f"Saving ({writing_mode}): '{path_schema}'")

    # if we are appending (not overwriting), the empty check must be done
    # BEFORE writing (not after)
    if append and (not allow_empty):
        is_empty = df.limit(1).count() == 0
        msg = f"About to append and empty dataset: {path_schema}"
        assert not is_empty, msg

    if isinstance(partition_by, str):
        partition_by = [partition_by]

    ts = time.time()

    _write_blob_internal(
        df=df,
        path_schema=path_schema,
        writing_mode=writing_mode,
        partition_by=partition_by,
    )

    te = time.time()
    msg_dur = time.strftime("%H:%M:%S", time.gmtime(te - ts))
    log.info(f"Completed save in: {msg_dur}")

    # why are we checking for empty data after writing and not before:
    # to make sure spark does not re-calculate the logic twice, this way
    # is cheaper
    if (not append) and (not allow_empty):

        # sometimes when empty dataset is written the schema cannot be inferred
        # when reading it, and the reading breaks, handle that here
        try:
            is_empty = spark.read.parquet(path_schema).limit(1).rdd.isEmpty()
        except Exception as ex:
            log.warning(ex)
            is_empty = df.limit(1).count() == 0

        msg = f"Empty data was written: {path_schema}"
        assert not is_empty, msg


def write_blob_csv(
    df: SparkDataFrame,
    path: str,
    compression: Optional[str] = None,
    encoding: Optional[str] = "utf8",
) -> None:
    """
    Simple wrapper around writing CSV to blob.

    Parameters
    ----------
    df: spark dataframe to store
    path: where to write on blob
    compression: compression level (default to None)
    encoding: encoding (UTF-8 is default to allow for French characters)

    Returns
    -------
    None
    """

    path_schema, _ = get_paths(path)

    # TODO: this is a compatibility fix to be able to take paths without root
    path_schema = fix_path_compatability(path_schema)

    # some validation we are not about to write where we are not allowed
    check_if_io_to_test_folder(path_schema)

    msg_compression = "" if compression is None else f" ({compression})"

    log.info(f"Writing CSV{msg_compression}: {path_schema}")

    _write_blob_csv_internal(
        df=df,
        path_schema=path_schema,
        encoding=encoding,
        compression=compression,
    )


def write_blob_csv_no_sub_folder(
    df: SparkDataFrame,
    path: str,
    encoding: Optional[str] = "utf8",
):
    """
    Simple wrapper around writing CSV to blob that writes it in a way that
    there is no sub-folder created by spark and no additional files.

    Parameters
    ----------
    df: spark dataframe to store
    path: where to write on blob
    encoding: encoding (UTF-8 is default to allow for French characters)

    Returns
    -------
    None
    """

    path_schema, _ = get_paths(path)

    # TODO: this is a compatibility fix to be able to take paths without root
    path_schema = fix_path_compatability(path_schema)

    # some validation we are not about to write where we are not allowed
    check_if_io_to_test_folder(path_schema)

    log.info(f"Writing CSV (no sub folder): {path_schema}")

    path_to_write_schema, path_to_write_local = get_temp_paths()

    # use the standard write CSV to write to temp folder first
    # note: we dont allow compression in this method
    _write_blob_csv_internal(
        df=df,
        path_schema=path_to_write_schema,
        encoding=encoding,
        compression=None,
    )

    # get the full temp path to copy over the data to final path
    path_csv_temp = get_full_csv_path(path_to_write_schema)
    path_csv_temp_schema, _ = get_paths(path_csv_temp)

    from spaceprod.utils.dbutils import get_dbutils

    db = get_dbutils()

    db.fs.rm(path_schema, recurse=True)
    db.fs.cp(path_csv_temp_schema, path_schema)


def get_temp_paths(
    path: Optional[str] = None,
    name: Optional[str] = None,
    extension: Optional[str] = None,
):
    """
    Creates a temp path on blob.
    Parameters
    ----------
    path: if supplied the root of the folder to use, otherwise default used
    name: name (suffix) for the file, none is used if not supplied

    Returns
    -------
    tuple of schema path and local path for the same path
    """
    path = path or PATH_TEMP_CACHE_SCHEMA + "/"
    extension = extension or ""

    path_schema, path_local = get_paths(path)

    ts = dt.datetime.now().strftime("%Y%m%dT%H%M%S")
    rand = uuid.uuid4().hex + str(time.time())
    m = hashlib.md5()
    m.update(rand.encode("utf-8"))
    hash_ = m.hexdigest()

    if name is None:
        suffix = f"{ts}_{hash_}"
    else:
        suffix = f"{name}_{ts}_{hash_}"

    path_to_write_schema = path_schema + "_" + suffix + extension
    path_to_write_local = path_local + "_" + suffix + extension

    return path_to_write_schema, path_to_write_local


def backup_on_blob(
    spark: SparkSession,
    df: SparkDataFrame,
    path: Optional[str] = None,
    backup_name: Optional[str] = None,
    allow_empty: Optional[str] = False,
    partition_by: Optional[List[str]] = None,
    get_path: Optional[bool] = False,
):
    """
    Writes the dataset in parquet format to a temp path on blob for purposes
    of backup and to materialize the data during processing.

    Parameters
    ----------
    spark: instance of spark
    df: spark dataframe to write
    path: root path of backup folder, default is used if not supplied
    backup_name: the name of backup (suffix) none is used if not supplied
    allow_empty: if False, will break if backup is empty
    partition_by: if supplied list of columns, will be partioned by them
    get_path: if True, will return the backup path instead of data

    Returns
    -------
    reads back the data from the back-up and returns as spark DF
    """

    var_name = "SPACE_SKIP_BACKUP_ON_BLOB"
    if os.getenv(var_name) is not None:

        msg = f"cannot combine '{var_name}' and 'get_path'=True"
        assert not get_path, msg

        log.info(f"skipping 'backup_on_blob' as '{var_name}' env var is set")
        # replacing write with spark's .cache()
        import pyspark

        df.rdd.persist(pyspark.StorageLevel.DISK_ONLY)
        return df

    path_to_write_schema, path_to_write_local = get_temp_paths(
        path=path,
        name=backup_name,
    )

    msg = (
        "You are overwriting a backup! Pass a different 'backup_name' "
        "or don't pass one for a random string"
    )

    assert not os.path.exists(path_to_write_local), msg
    assert not exists_blob(spark, path_to_write_schema), msg

    log.info(f"Caching data here: {path_to_write_schema}")

    ts = time.time()

    partition_by = partition_by or []

    _write_blob_internal(
        df=df,
        path_schema=path_to_write_schema,
        writing_mode="overwrite",
        partition_by=partition_by,
    )

    if not allow_empty:
        is_empty = spark.read.parquet(path_to_write_schema).limit(1).count() == 0
        msg = f"Empty data was written: {path_to_write_schema}"
        assert not is_empty, msg

    te = time.time()
    msg_dur = time.strftime("%H:%M:%S", time.gmtime(te - ts))

    log.info(f"Completed backup in: {msg_dur}")

    df = spark.read.parquet(path_to_write_schema)

    if get_path:
        return path_to_write_schema

    return df


def read_blob_inside_worker(path: str) -> pd.DataFrame:
    """
    a function to read the parquet file into Pandas inside
    a worker
    Used in pandas UDF parallelization to pass large datasets

    writing them as partitioned parquet table before calling udf and than
    calling this function to read each partition individually

    Will work as long as the slice of data will fit in memory of the worker
    machine
    """
    from inno_utils.azure import download_from_blob

    path_schema = get_paths(path)[0]
    path_fix = fix_path_compatability(path_schema=path_schema, full_path=False)
    path_posix = Path(path_fix).as_posix()

    log.info(f"Reading: '{path_posix}'")

    tmp_file_name = _create_temp_local_path(
        prefix="temp_parquet",
        extension=None,
    )

    def read_handler(path_local: str):
        return pd.read_parquet(path_local)

    pdf = _download_and_read_a_file(
        path_remote=path_posix,
        path_local=tmp_file_name,
        read_handler=read_handler,
    )

    return pdf


def df_to_pdf(df: SparkDataFrame):
    """
    converts a DF into a PDF via writing it to parquet and downloading it
    to local first (minimizes the chance of an OOM / Java heap space error)
    """

    from inno_utils.azure import download_from_blob

    df = df.coalesce(1).repartition(1)
    rand = uuid.uuid4().hex + str(time.time())
    m = hashlib.md5()
    m.update(rand.encode("utf-8"))
    hash_ = m.hexdigest()

    # TODO: define centrally
    path = f"adhoc/space_prod/temp_cache/test_write/{hash_}"

    path_schema, _ = get_paths(path)

    path_schema = fix_path_compatability(path_schema)

    _write_blob_internal(
        df=df,
        path_schema=path_schema,
        writing_mode="overwrite",
        partition_by=[],
    )
    log.info(f"Written! Start downloading parquet from here: {path_schema}")

    tmp_file_name = f"/tmp/{hash_}"

    download_from_blob(path, tmp_file_name)
    log.info(f"Downloaded! Start reading parquet from here: {tmp_file_name}")

    pdf = pd.read_parquet(tmp_file_name)

    return pdf


def folder_exists_on_blob(path: str) -> bool:
    """
    Checks if the path exists on blob (even if it is not a dataset)

    Parameters
    ----------
    path : any path on blob

    Returns
    -------
    True if path exists, False otherwise
    """

    from spaceprod.utils.dbutils import get_dbutils

    path = fix_path_compatability(path)

    du = get_dbutils()
    list_files = du.fs.ls(os.path.dirname(path))
    list_paths = [Path(x.path).as_posix() for x in list_files]
    return path in list_paths


def is_logging_summary_statistics_suppressed() -> bool:
    """
    we can suppress logging of summary stats using an env var
    this function checks if it is suppressed
    """
    var_name = "SPACE_SUPPRESS_SUMMARY_STATS_LOGGING"
    if os.getenv(var_name) is not None:
        log.info(f"Skipping summary stats logging as var is set: {var_name}")
        return True

    return False
