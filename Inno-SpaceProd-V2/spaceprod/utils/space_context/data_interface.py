import os
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from tests.data_validation.validator import DatasetValidator

from inno_utils.loggers import log
from spaceprod.utils.data_helpers import (
    backup_on_blob,
    read_blob,
    read_blob_csv,
    read_blob_dict_paths,
    read_blob_txt_files,
    write_blob,
    write_blob_csv,
    write_blob_csv_no_sub_folder,
)
from spaceprod.utils.imports import F, SparkDataFrame
from spaceprod.utils.space_context.data_config_validator import (
    DATA_CONFIG_PATHS,
    PATH_ROOT,
    PATH_RUN_FOLDERS,
    REGIONS,
    DataConfigValidator,
)
from spaceprod.utils.space_context.spark import spark


class DataIO(DataConfigValidator):
    """
    Object that is aware of both the config and context attributes.
    Allows us to resolve paths and read / write data on the fly.
    Acts as a data IO layer that is hooked up to the context for ease
    of access.
    """

    def __init__(self, context):
        self.ctx = context

    def read(
        self,
        dataset_id: str,
        regions: Optional[List[str]] = None,
    ) -> SparkDataFrame:
        """
        Universal endpoint to read any dataset that automatically determines
        where the dataset is located based on supplied Dataset ID which should
        come from the config.

        For example:

        Dataset should be configured under this YML file:
         - pipeline/config/clustering/some_clustering_config.yml

        like this (inside tha above YML file:

            some_header:
                my_dataset_id: some/path/to/my/dataset


        than to read this dataset you can use:
        >>> from spaceprod.utils.space_context import context
        >>> df_my_data = context.data.read("my_dataset_id")

        please note, this should be added to 'DATA_CONFIG_PATHS' object above:
        >>> ('clustering', 'some_clustering_config', 'some_header')

        Parameters
        ----------
        dataset_id: dataset ID from any of the YML configs to read
        regions: List of regions to read if this is a region-based dataset

        Returns
        -------
        Spark dataframe
        """

        # make sure the regions arg is set correctly if supplied
        if regions is not None:
            msg = f"'regions' arg must be in: {REGIONS}"
            assert len(regions) > 0, "you cannot pass empty 'regions' list"
            assert self._is_valid_region_list(regions), msg

        # resolve the path
        path = self.path(dataset_id, regions=regions)

        # get various flags to be used in the below logic
        is_test = self._is_integ_test()
        is_mocked = self._is_dataset_mocked(dataset_id=dataset_id)
        is_external = self._is_dataset_external(dataset_id=dataset_id)

        # now check if we are running a test get a test dataset
        # please note, this is not a good practice to have test-purpose code
        # inside your main code as it defeats the purpose of testing. in our
        # case we get great benefits of saving overhead compute on writing
        # these mocked datasets to disc prior to test
        if is_test and is_mocked:
            log.info(f"running integ test, returning mock for: '{dataset_id}'")

            df = self._return_mock_dataset_as_part_of_integ_test(
                dataset_id=dataset_id,
                regions=regions,
            )

            # validate data if required
            # validating only external datasets
            if is_external:
                self._validate_external_dataset(df=df, dataset_id=dataset_id)

            # return the test mock dataset
            return df

        # determine the handler how to read this dataset
        # TODO: this is not a good practice,
        #  ideally we should data-contract-driven read handlers
        read_data, kwargs = self.determine_read_handler(
            resolved_path=path,
            regions=regions,
        )

        try:
            df = read_data(**kwargs)
        except Exception as ex:
            tb = traceback.format_exc()

            msg = f"""            
            COULD NOT LOAD DATA!              
            exception: {ex}
            traceback: {tb}
            Dataset ID '{dataset_id}' was resolved to this path: '{path}'.
            See exception and traceback above.            
            PLEASE NOTE: 
            The dataset must be a valid parquet dataset saved on Blob.
            All other formats (XLSX, Pickle, etc) are not supported ATM.
            """

            raise Exception(msg)

        # validate data if required (if it is an external dataset)
        if is_external:
            self._validate_external_dataset(df=df, dataset_id=dataset_id)

        # return the results
        return df

    def write(
        self,
        dataset_id: str,
        df: SparkDataFrame,
        partition_by: Optional[Union[str, List[str]]] = None,
        allow_empty: Optional[bool] = False,
        append: Optional[bool] = False,
    ) -> None:
        """
        Universal writing interface for datasets
        data should be written only using this method
        """

        # find the full path to the dataset
        path = self.path(dataset_id)

        # currently we don't allow writing data to paths that are not
        # versioned by run ID
        self._break_if_unversioned_path(
            dataset_id=dataset_id,
            resolved_path=path,
        )

        # currently we don't allow writing data to paths that
        # have regional breakdown
        self._break_if_regional_path(dataset_id, path)

        # currently we don't allow writing data in format other than
        # .parquet using this method
        format = self.infer_format(path)

        msg = f"""
        Looks like you are writing to a non-parquet path: '{path}'.
        Dataset ID: '{dataset_id}'.
        Either change the handler to .write_csv(...)
        or change the path in the config to be parquet.
        """

        assert format == "parquet", msg

        write_blob(
            spark=spark,
            df=df,
            path=path,
            partition_by=partition_by,
            allow_empty=allow_empty,
            append=append,
        )

    def write_csv(
        self,
        dataset_id: str,
        df: SparkDataFrame,
        compression: Optional[str] = None,
        encoding: Optional[str] = "utf8",
        no_sub_folder: Optional[bool] = False,
    ):

        # find the full path to the dataset
        path = self.path(dataset_id)

        # currently we don't allow writing data to paths that
        # have regional breakdown
        self._break_if_regional_path(dataset_id, path)

        # currently we don't allow writing data to paths that are not
        # versioned by run ID
        self._break_if_unversioned_path(
            dataset_id=dataset_id,
            resolved_path=path,
        )

        # currently we don't allow writing data in format other than
        # .csv using this method
        format = self.infer_format(path)

        msg = f"""
        Looks like you are writing to a non-csv path: '{path}'
        Either change the handler to .write(...)
        or change the path in the config to be CSV
        """

        assert format == "csv", msg

        if no_sub_folder:
            write_blob_csv_no_sub_folder(
                df=df,
                path=path,
                encoding=encoding,
            )

            return

        write_blob_csv(
            df=df,
            path=path,
            compression=compression,
            encoding=encoding,
        )

    def backup(
        self,
        df: SparkDataFrame,
        dataset_id: Optional[str] = None,
        backup_name: Optional[str] = None,
    ):

        path = self.path(dataset_id) if dataset_id else None

        self._break_if_regional_path(dataset_id, path)

        backup_on_blob(
            spark=spark,
            df=df,
            path=path,
            backup_name=backup_name,
        )

    def _break_if_regional_path(
        self,
        dataset_id: str,
        path: Union[str, Dict[str, str]],
    ) -> None:
        """
        a wrapper around raising an informative error when we don't
        want to allow paths with regional breakdown
        """
        # if this is a region-based path, break
        if isinstance(path, dict):
            msg = f"""
            Dataset id: '{dataset_id}' is using regional breakdown
            We dont' allow writing to such datasets currently.
            Please provide a dataset ID that does not have regional 
            breakdown in config.
            Alternatively, if you are using .backup(...) function 
            you can omit the 'dataset_id' argument 
            """

            raise Exception(msg)

    def _break_if_unversioned_path(
        self,
        dataset_id: str,
        resolved_path: str,
    ):

        # path MUST be within the run folder
        # which can be either full DBFS path or blob path
        is_versioned = self._is_versioned_path(
            path=resolved_path, run_id=self.ctx.run_id
        )

        # currently we don't allow writing data to paths that are not
        # versioned by run ID
        msg = f"""
        The following dataset ID: '{dataset_id}' has been resolved
        to a path that is not versioned AND/OR it is outside of the current
        run folder, currently we don't allow writing to un-versioned paths or
        writing outside of the run folder. 
        Please make sure your path in YAML starts with 
            '{PATH_RUN_FOLDERS}/{{run_id}}/'
            OR 
            '{PATH_ROOT}/{PATH_RUN_FOLDERS}/{{run_id}}/'        
        Path resolved to: '{resolved_path}'.
        Current run folder: '{self.ctx.run_folder}'.
        """

        assert is_versioned, msg

    def path(
        self,
        dataset_id: str,
        regions: Optional[List[str]] = None,
        resolve: Optional[bool] = True,
    ) -> Union[str, Dict[str, str]]:
        """
        Searches for the dataset path by supplied dataset ID across ALL
        configs where we have dataset configured, and resolves the full
        versioned path to this dataset.

        Parameters
        ----------
        dataset_id: dataset ID to search for (key from config)
        regions: if this is a region-based dataset, supply list of regions
        resolve: if False, will NOT resolve the path

        Returns
        -------
        (resolved) path to dataset
        """

        # get all datasets configured throughout all configs
        config_data = self.get_data_config(self.ctx.config, self.ctx.run_id)

        for config_address, config_address_value in config_data.items():

            if dataset_id in config_address_value.keys():
                path_unresolved = config_address_value[dataset_id]

                if resolve:
                    path = self._resolve_path(path_unresolved, self.ctx.run_id)
                else:
                    path = path_unresolved

                # convert to schema path
                path_schema = self._path_to_schema(path)

                # if this is a region-based path, get the right regions
                if isinstance(path_schema, dict):

                    msg = f"""
                    This dataset: '{dataset_id}' is broken down by region.
                    Please supply 'regions' argument as a list of strings
                    representing regions - all lower case, for e.g.:
                    >>> regions=["ontario", "west"] 
                    """

                    assert regions is not None, msg

                    path_schema = {k: v for k, v in path_schema.items() if k in regions}

                else:
                    msg = f"""
                    This dataset: '{dataset_id}' is NOT broken down by region.
                    Please do NOT 'regions' argument to this function call
                    """

                    assert regions is None, msg

                return path_schema

        msg_list = "\n".join([f"- " + ".".join(x) for x in DATA_CONFIG_PATHS])

        msg = f"""
        Dataset ID '{dataset_id}' not found in config.
        Please make sure it exists in the config in one of the following
        Sections:\n{msg_list}
        """

        raise Exception(msg)

    def determine_read_handler(
        self,
        resolved_path: Union[str, Dict[str, str]],
        regions: Optional[List[str]] = None,
    ) -> Tuple[Callable, Dict[str, Any]]:
        """
        Returns which handler should be used to read the dataset as part of the
        IO layer.

        TODO: Currently the handler is inferred only from path, which is not
         ideal. Normally formats should be part of data contracts and thus
         handlers determined based on data contracts

        Parameters
        ----------
        resolved_path: path to the dataset

        Returns
        -------
        handler to read the data
        """

        # a shortcut
        path = resolved_path

        if isinstance(path, dict):
            formats = [self.infer_format(x) for x in path.values()]
            msg = f"We don't allow mixed data formats here: {path}"
            assert len(set(formats)) == 1, msg
            format = formats[0]
            msg = f"This is a region-based path, please supply 'regions': {path}"
            assert regions is not None, msg

            # we don't allow CSV formats in paths with regional breakdown
            fimct_mapping = {
                "parquet": read_blob_dict_paths,
            }

            args = {
                "spark": spark,
                "dict_paths": path,
                "col_name": "REGION",
                "filter_keys": regions,
            }

        else:
            format = self.infer_format(path)

            fimct_mapping = {
                "csv": read_blob_csv,
                "parquet": read_blob,
                "txt": read_blob_txt_files,
            }

            args = {
                "spark": spark,
                "path": path,
            }

        msg = f"""
        This format ('{format}') is not supported for this dataset.
        Only supports: '{fimct_mapping.keys()}'
        Dataset path(s): {path}
        """

        assert format in fimct_mapping.keys(), msg

        return fimct_mapping[format], args

    def infer_format(self, resolved_path: str):
        """
        Attempts to determine the format of the dataset based on
        dataset path. This can work since we have data path naming conventions
        in place, for e.g. we can make a dataset CSV if the path
        ends with ".csv"
        """

        if resolved_path.endswith(".csv"):
            return "csv"
        else:
            from inno_utils.azure import get_files_in_blob_folder

            files = get_files_in_blob_folder(resolved_path)
            if any(file.endswith(".txt") for file in files):
                return "txt"

        return "parquet"

    def _validate_external_dataset(self, df: SparkDataFrame, dataset_id: str) -> None:
        """
        Validates the given external dataset if there is a data contract
        present
        """

        # see if there is a data contract for hti external dataset
        from spaceprod.utils import data_contracts_external as dcs

        data_contract: Union[DatasetValidator, None] = getattr(dcs, dataset_id, None)

        # run validation if there was a data contract found,
        # otherwise throw an warning
        if data_contract is not None:
            data_contract.validate(df=df)
        else:

            msg = (
                f"No data contract found for this external dataset: '{dataset_id}', "
                f"consider implementing it under "
                f"'spaceprod/utils/data_contracts_external.py'"
            )

            log.warning(msg)

    def _return_mock_dataset_as_part_of_integ_test(
        self, dataset_id: str, regions: List[str]
    ) -> SparkDataFrame:
        """
        A "fake" read handler for returning mock data as part of Integration
        Test.

        It gets data from Python object under 'tests.mock_data.MockData'
        for a given dataset ID.

        It mimics the behaviour of the real read handler by adding the
        REGION column if its a regional dataset

        Parameters
        ----------
        dataset_id : str
            dataset_id to return

        regions : List[str]
            if supplied will add the region column
            (NOTE: only 1 region is supported in mock datasets)

        Returns
        -------
        mock data
        """

        from tests.mock_data import integration_test_mock_data

        df = getattr(integration_test_mock_data, dataset_id, None)

        msg = f"""
        Looks like you are running an integ test and the following 
        dataset is not in the 'tests.mock_data.MockData' object.
        It must be one of the properties in the object and its name
        must match the dataset ID: '{dataset_id}'
        """

        assert df is not None, msg

        # check if region(s) was provided. In a test only 1 region
        # can be provided.
        # whether a region SHOULD be provided has been already verified
        # above when self.path(...) was called
        if regions is not None and len(regions) > 0:

            msg = f"""
            In an integ test we can only test with 1 region, please
            make sure you supply only 1 region to 'context.data.read'
            and check your scope.yml config to make sure it has 1 region.
            """

            assert len(regions) == 1, msg

            # add the dummy region to data to replicate the actual
            # behaviour of the read_blob_dict_paths
            df = df.withColumn("REGION", F.lit(regions[0]))

        return df

    def _is_integ_test(self):
        """
        checks if we are running an integration test
        NOTE: this is not a good practice to have this in the logic.
        In our case it might be ok as long as we are only using it to
        determine whether mock data or real data should be returned.
        In our case it behaves as "monkey patching" which should be fine.
        But please DON'T use it for anything else.
        """

        var = "SPACE_IS_INTEG_TEST"
        is_test = os.getenv(var) is not None

        if is_test:
            log.info(f"We are running Integ test because env var '{var}' is set")

        return is_test

    def _is_dataset_external(self, dataset_id: str):
        """
        determines if a given dataset is external or generated based on
        the supplied 'dataset_id'.

        - If returns True: the dataset is external, i.e. NOT generated by our
          pipeline

        - If returns False: the dataset is generated, i.e. it is generated by
          one of the tasks of our pipeline
        """

        is_external = dataset_id in self.ctx.config["data"]["external"].keys()

        return is_external

    def _is_dataset_mocked(self, dataset_id: str):
        """
        determines if a given dataset is mocked,
        i.e. it has a corresponding property the "official" MockData object
        """

        from tests.mock_data import MockData

        is_mocked = getattr(MockData, dataset_id, None) is not None

        return is_mocked
