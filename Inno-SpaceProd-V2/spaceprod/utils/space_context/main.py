import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from inno_utils.loggers import log
from inno_utils.loggers.log import format_title
from pyspark import RDD, Row
from spaceprod.utils.data_helpers import (
    exists_blob,
    fix_path_compatability,
    folder_exists_on_blob,
    write_blob,
    write_blob_csv,
)
from spaceprod.utils.dbutils import get_dbutils
from spaceprod.utils.space_context.data_config_validator import (
    PATH_ROOT,
    PATH_RUN_FOLDERS,
)
from spaceprod.utils.space_context.data_interface import DataIO
from spaceprod.utils.space_context.helpers import read_config
from spaceprod.utils.space_context.run_versioning import (
    generate_run_id,
    generate_username,
)
from spaceprod.utils.space_context.spark import spark


class Singleton(object):
    """a basic implementation of singleton"""

    def __new__(cls, *args, **kwds):
        it = cls.__dict__.get("__it__")
        if it is not None:
            return it
        cls.__it__ = it = object.__new__(cls)
        it.init(*args, **kwds)
        return it

    def init(self, *args, **kwds):
        pass


class Context(Singleton):
    """
    A singleton object that is shared across a particular execution run and is
    accessible to every task or business logic module
    Used to store all the information required during the execution of a task.
    Also used to access the data IO layer at `.data`
    """

    def __init__(
        self,
        run_id: Optional[str] = None,
        env: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ):

        self.env = env or generate_username()
        self.run_id = run_id or generate_run_id()
        self._config = None or config

        # Universal endpoint to read any dataset that automatically determines
        # where the dataset is located based on supplied Dataset ID which
        # should come from the config.
        # see docstring for the DataIO object for more details
        self.data = DataIO(self)

        # spit welcome message
        log.info(self)

    def __repr__(self):

        ttl = format_title("Welcome to Space Prod session!")

        msg = f"""
        {ttl}
        Your current Run ID: {self.run_id}
        Your current Run folder: {self.run_folder}
        """
        return msg

    @property
    def run_folder(self) -> str:
        """the root path of the current run"""
        # resolve the full path to the run folder

        run_id = self.run_id
        path_run_folder = Path(PATH_ROOT, PATH_RUN_FOLDERS, run_id).as_posix()
        return path_run_folder

    @property
    def config(self) -> Dict[str, Any]:
        """
        Loads the config from disk (if it has not been loaded already)
        We only want to load it ONCE per session
        """
        if self._config is not None:
            return self._config

        # read, parse and validate config
        config = read_config()

        # validate the data portion of the config
        self.data.get_data_config(config, self.run_id)

        # assign config dict to the internal variable for quick access later
        self._config = config

        return config

    def reload_config_from_disc(self):
        """
        Force-reload config from disc, for cases when the config
        is updated AFTER the session has been created (never happens during
        regular pipeline run, only happens during interactive development,
        which is when this method can be used)
        """
        self._config = read_config()

    def serialize_session(self) -> Dict[str, str]:
        """
        Serializes the session info into a dictionary to be able to
        store on disc and resume runs in future
        """
        return {"run_id": self.run_id, "env": self.env, "config": self.config}

    def dump_session(self) -> None:
        """
        Dumps your current run session and configuration to the run folder
        so that you can resume at a later time using:
        >>> context.reload_context_from_run_folder("dbfs:/some/run/folder")
        Contantly used in operations and debugging
        """

        path_run_folder = self.run_folder

        path_context_session_dump = Path(path_run_folder, "session").as_posix()
        log.info(f"Dumping session here: {path_context_session_dump}")

        # produce content
        ser_session = self.serialize_session()
        json_session = json.dumps(ser_session, indent=2)
        log.info(f"Dumping session with Run ID: {self.run_id}")

        # convert content to RDD to be able to save on blob
        # not using regular Python file handlers as this works equally well
        # when executing on both Databricks and Databricks-Connect
        rdd_session: RDD = spark.sparkContext.parallelize(Row(json_session))

        # delete existing dump in case if it exists
        du = get_dbutils()

        if du is None:

            msg = f"""
            Cannot not dump session. Make sure you are not running this 
            inside a worker
            """

            raise Exception(msg)

        du.fs.rm(path_context_session_dump, True)

        # perform dump
        rdd_session.repartition(1).saveAsTextFile(path_context_session_dump)

    def reload_context_from_run_folder(self, path_run_folder: str) -> None:
        """Loads existing session context from a run folder to resume a run"""
        path_context_session_dump = Path(path_run_folder, "session").as_posix()
        log.info(f"Start loading session and config from: {path_run_folder}")

        pdf_sess = spark.read.text(path_context_session_dump).toPandas()

        sess = json.loads("\n".join(pdf_sess.to_dict("list")["value"]))

        run_id = sess["run_id"]
        log.info(f"Loading run ID: {run_id}")

        self.__init__(**sess)

    def clone_run(
        self,
        new_run_suffix: str,
        specific_data_config_addresses: Optional[List[str]] = None,
    ):
        """
        "clones" or "multiplies" the current run in the context to a new
        folder and new run id.

        Details on why we need it and how to use it in Confluence here:
        https://sobeysdigital.atlassian.net/wiki/spaces/SP/pages/5444370468/Accessing+Data+Config+and+Run+Context#Multiplying-%2F-cloning-runs

        What it does:

        - creates a new run ID by taking your original run ID and appending
          the new_run_suffix to the end of it.

        - creates a new run folder that is named accordingly.

        - re-dumps the original session and config information with the
          newly-generated run ID into the new run folder.

        - copies over the data from the original run folder to the new run
          folder.

        Parameters
        ----------
        new_run_suffix : str
            a suffix to generate the new run ID (will be appended to the
            current run id, i.e. self.run_id)

        specific_data_config_addresses: Optional[List[str]]
            a list of data config addresses to use when finding the data
            to copy. If not supplied, ALL available data config addresses will
            be used. Useful when you want to clone the run only "partially".

        Returns
        -------
        None
        """

        # all external data must live at this address
        addr_external_data = "data.external"

        if specific_data_config_addresses is not None:
            msg = f"You cannot pass '{addr_external_data}' in 'specific_data_context'"
            assert addr_external_data not in specific_data_config_addresses, msg

        # in the suffix only allowing numbers, letters and underscores
        is_valid = re.match(r"^[A-Za-z0-9_]+$", new_run_suffix) is not None

        msg = f"""
        The following 'new_run_suffix' is invalid, 
        only allowing alphanumeric characters and underscores. No spaces.
        '{new_run_suffix}'
        """

        assert is_valid, msg

        # access require attributes
        config = self.config
        run_id = self.run_id
        run_folder = self.run_folder
        conf_data = self.data.get_data_config(config, run_id)

        # ensure that the user is not passing non-existent data config
        # addresses (if applicable)
        if specific_data_config_addresses is not None:
            addresses_avail = list(conf_data.keys())
            addresses_avail = [x for x in addresses_avail if x != addr_external_data]
            addresses_bad = set(specific_data_config_addresses) - set(addresses_avail)
            if len(addresses_bad) > 0:

                msg_addr = "\n".join(addresses_avail)

                msg = f"""
                \nFound some invalid data config addresses passed: {list(addresses_bad)}
                \nYou can pass any of the following:
                \n{msg_addr}
                """

                raise Exception(msg)

        # determine the new run folder path
        run_folder_new = f"{run_folder}_{new_run_suffix}"
        msg = "This folder must exist, make sure you actually ran the run: {run_folder}"
        assert folder_exists_on_blob(run_folder), msg

        # the new run id should follow the same pattern
        run_id_new = os.path.basename(run_folder_new)

        # check to ensure that such run folder does not exist
        msg = "This folder must NOT exist: {run_foler_new}"
        assert not folder_exists_on_blob(run_folder_new), msg

        # below we want to recursively copy all data from old
        # we are NOT using db_utils.fs.cp here because it is not efficient
        # on large datasets, therefore we use the standard Spark's
        # read/write interface to "re-write" data using distributed compute

        for address, conf_data_section in conf_data.items():

            if address == addr_external_data:
                continue

            if specific_data_config_addresses is not None:
                if address not in specific_data_config_addresses:
                    msg = f"This address is not requested, skipping: {address}"
                    log.info(msg)
                    continue

            msg = f"Start migrating data from: '{address}'"
            log.info(format_title(msg))

            for dataset_id, dataset_path in conf_data_section.items():

                # since we don't have regional datasets as part of
                # generated datasets, we can explicitly put the below
                # restriction. We only have regional datasets as part of
                # external datasets.
                if isinstance(dataset_path, dict):
                    msg = f"Regional paths are not supported in versioned data: {dataset_id}: {dataset_path}"
                    raise Exception(msg)

                # determine where to copy data from using the standard path
                # resolution function
                path_from = self.data.path(dataset_id)
                path_from = fix_path_compatability(path_from)

                path_to = path_from.replace(run_folder, run_folder_new)

                msg = f"""
                New data path comes out to be no different from the old data 
                data path. Please use a different 'new_run_suffix' value.
                - data path: {path_from}
                - old run folder: {run_folder}
                - new run folder: {run_folder_new}
                """

                assert path_to != path_from, msg

                # it could be the case that the source data does not exist
                # this simply means that this is not a complete run, but we
                # still would like to be able to clone, so we just skip
                # occurrences when the source data does not exist
                if not exists_blob(spark, path_from):
                    msg = f"skipping (does not exist): {dataset_id}: {dataset_path}"
                    log.info(msg)
                    continue

                # read the data using the standard reading interface
                # (universal for for both parquet and csv)
                df = self.data.read(dataset_id)

                # now we need to write the data to the new run folder
                # writing data is not universal, thus we need to infer format
                # and use the righ thandler
                format_ = self.data.infer_format(dataset_path)

                # mapping that maps which write handler to be used depending
                # on the format
                func_mapping = {
                    "csv": write_blob_csv,
                    "parquet": write_blob,
                }

                # NOTE: we are writing with all default params / args here
                # which may or may not be original way these dataset were
                # written at this point there is no way to cleanly infer these
                # params. But is is ok for now.

                # mapping that maps the format to the args that need to be
                # passed
                args_mapping = {
                    "csv": {"df": df, "path": path_to},
                    "parquet": {
                        "spark": spark,
                        "df": df,
                        "path": path_to,
                        "allow_empty": True,
                    },
                }

                func = func_mapping[format_]
                args = args_mapping[format_]

                # write the data to new path
                func(**args)

        # next we need to re-dump the session with new run ID to the
        # new run folder
        self.run_id = run_id_new
        self.dump_session()
