import re
from pathlib import Path
from typing import Any, Dict, Tuple, Union, List

from spaceprod.utils import ROOT_PATH_BLOB_LOCAL, ROOT_PATH_BLOB_SCHEMA
from spaceprod.utils.data_helpers import fix_path_compatability, get_paths

from spaceprod.utils.space_context.run_versioning import RUN_ID_PATTERN


# the object holds all config locations where the datasets are stored
# if you are introducing a new place where a dataset would be stored,
# please make sure to add it here.
# you DO NOT need to change anything here if you are introducing a new
# dataset that already lives in one of the below locations
# structure:
# ("folder OR file name", "file name OR config section", "config section")
DATA_CONFIG_PATHS = [
    ("data", "external"),
    ("clustering", "need_states_config", "need_states_outputs"),
    ("clustering", "internal_clustering_config", "internal_clustering_outputs"),
    ("clustering", "internal_profiling_config", "internal_profiling_outputs"),
    ("clustering", "external_clustering_config", "external_clustering_outputs"),
    ("clustering", "concatenate_clustering_config", "concatenate_clustering_outputs"),
    ("clustering", "dashboard_views_config", "dashboard_outputs"),
    ("elasticity", "micro_elasticity_config", "elasticity_outputs"),
    ("adjacencies", "adjacencies_config", "adjacencies_outputs"),
    ("optimization", "integrated_optimization_config", "outputs"),
    ("optimization", "sense_check_config", "outputs"),
    ("ranking", "ranking_config", "outputs"),
    ("system_integration", "system_integration_config", "outputs"),
]

# out of the above config paths, which config paths hold external data
EXTERNAL_DATA_CONFIG_PATHS = [
    ("data", "external"),
]

# datasets broken down by region should have the following sub-keys:
REGIONS = ["atlantic", "ontario", "west", "quebec"]

# run folder information:
PATH_ROOT = ROOT_PATH_BLOB_SCHEMA
PATH_RUN_FOLDERS = "sobeys_space_prod"


class DataConfigValidator:
    def _resolve_single_path(self, unresolved_path: str, run_id: str) -> str:
        """
        very primitive logic to resolve a templated path from config to a
        version path using variables
        TODO: can become more complex in future - currently only supports 'run_id' (Jinja)
        """

        if unresolved_path.__contains__("{run_id}"):
            resolved_path = unresolved_path.format(run_id=run_id)
        else:
            resolved_path = unresolved_path

        if resolved_path.__contains__("{") or resolved_path.__contains__("}"):
            msg = f"""
            Some variables were not resolved, 
            please check your config path: '{resolved_path}'
            """

            raise Exception(msg)

        return resolved_path

    def _resolve_path(
        self,
        unresolved_path: Union[str, Dict[str, str]],
        run_id: str,
    ) -> Union[str, Dict[str, str]]:
        """
        a wrapper to resolve either single paths (string) or a region-specific
        path (dict)
        By "resolve" here means rendering the path(s) with "run_id" to
        make sure it is a versioned path.

        If the path is not versioned, it will be returned as-is.

        Parameters
        ----------
        unresolved_path: path that has '{run_id}' or any path
        run_id: run ID to use for resolving this path

        Returns
        -------
        same path(s) but with run ID if it has '{run_id}', otherwise paths
        as is.
        """

        if isinstance(unresolved_path, dict):

            res = {
                k: self._resolve_single_path(v, run_id)
                for k, v in unresolved_path.items()
            }
            return res

        return self._resolve_single_path(unresolved_path, run_id)

    def _path_to_schema(
        self, path: Union[str, Dict[str, str]]
    ) -> Union[str, Dict[str, str]]:
        """
        a wrapper that can translate a given path,
        either single paths (string) or a region-specific
        path (dict), to a "schema" format.
        For e.g. from /dbfs/hello/world/ to dbfs:/hello/world

        Parameters
        ----------
        path: path to be converted to schema path

        Returns
        -------
        schema path (single path, or region-specific path)
        """
        if isinstance(path, dict):
            res = {k: get_paths(v)[0] for k, v in path.items()}

            return res

        return get_paths(path)[0]

    def get_config_address_value(
        self,
        address: Tuple[str, ...],
        config: Dict[str, Any],
    ) -> Any:
        """
        returns value from config at a given address specified by a tuple.

        Its like quickly jumping to a desired folder in a hierarchy of
        files/folders when pasting an address into the URL bar.

        Parameters
        ----------
        address: address of the dict at which the desired value lives
        config: a (nested) dictionary to get the value from

        Returns
        -------
        a value at the given address
        """

        val = config
        for segment in address:
            val = val.__getitem__(segment)

        return val

    def _find_dataset_id(
        self,
        dataset_id: str,
        conf_data: Dict[str, Dict[str, str]],
    ) -> List[str]:
        """
        Finds the given dataset specified by the dataset ID in the data
        config.

        Creates a filtered dictionary of results from the data config where
        the dataset ID matches.

        Should return a list of length=1 assuming no keys are duplicated.

        Parameters
        ----------
        dataset_id:
            ID of the dataset to search

        conf_data:
            data config, a flat dictionary that maps dataset IDs to dataset
            paths

        Returns
        -------
        a filtered list of resulting dataset ID(s) from the data config
        where the path matches
        """

        list_found = [k for k, v in conf_data.items() if dataset_id in v.keys()]

        return list_found

    def _find_dataset_path(
        self,
        dataset_path: str,
        conf_data: Dict[str, Dict[str, str]],
    ):
        """
        Finds the given dataset specified by the dataset path in the data
        config.

        Creates a filtered dictionary of results from the data config where
        the path matches

        If the dataset path is seen more than once, will return a list of
        more than 1 record

        Parameters
        ----------
        dataset_path:
            path to a dataset

        conf_data:
            data config, a flat dictionary that maps dataset IDs to dataset
            paths

        Returns
        -------
        a filtered list of resulting dataset ID(s) from the data config
        where the path matches
        """

        list_found = [k for k, v in conf_data.items() if dataset_path in v.values()]

        return list_found

    def _is_valid_dataset_id(self, dataset_id: str):
        """
        checks if a dataset ID is a valid config parameter
        Currently no different from the ._is_valid_conf_param(...) logic
        but can be different in future if needed.

        Parameters
        ----------
        dataset_id: dataset id to validate

        Returns
        -------
        True if valid, False if invalid
        """
        if not self._is_valid_conf_param(dataset_id):
            return False

        return True

    def _is_valid_region_list(self, regions: List[str]):
        """
        checks a list of strings that supposed to represent a list of
        regions. Returns False if it is not a list of regions, otherwise True
        Parameters
        ----------
        regions: list of strings to validate

        Returns
        -------
        False if it is not a list of regions, otherwise True
        """
        list_check = list(set(regions) - set(REGIONS))
        if len(list_check) > 0:
            return False

        return True

    def _is_valid_dataset_path(self, path: Union[Dict[str, str], str]) -> bool:
        """
        checks if the dataset path is "valid"
        essentially it is a wrapper around various other validation functions

        it can check whether a path is valid for single path (string)
        or a region-based path (dict)

        Parameters
        ----------
        path: either a single path (str) or a region-based path (dict)

        Returns
        -------
        True if the dataset path is valid, otherwise False
        """

        # first check if this is a single path (str)
        # or a region-based path (dict)
        if isinstance(path, dict):

            # if this is a region-based path, check if
            # we have valid regions in this path
            if not self._is_valid_region_list(list(path.keys())):
                return False

            # now for each region check if at least one of the regional
            # paths are invalid
            if any([not self._is_valid_path(x) for x in path.values()]):
                return False

        # if this is a single path
        if isinstance(path, str):

            # we simply check if it is valid or not
            if not self._is_valid_path(path):
                return False

        # if all of the above checks passed, the dataset is valid (True)
        return True

    def _is_valid_conf_param(self, conf_param: str):
        """
        Checks if a string represents a valid config parameter
        A config parameter can only contain alpha-numeric characters.

        Parameters
        ----------
        conf_param: a string to check

        Returns
        -------
        True if the string can a config parameter, False if otherwise
        """
        return re.match("^[\w_]+$", conf_param) is not None

    def _is_valid_path(self, path: str) -> bool:
        """
        checks if a given path is a "valid" path by attempting to:
         - applying path compatability fix
         - converting the path to schema
         - resolving the path to be versioned (if applicable)

        if all of this is possible the path is considered "valid"

        Parameters
        ----------
        path: a string to check (path)

        Returns
        -------
        True if the path is considered "valid"
        """

        # first ensure we don't validate the "dbfs:/" prefix
        path = fix_path_compatability(path)
        path = path[6:]

        # make sure the path can be converted to schema path
        path = self._path_to_schema(path)

        # we allow .csv dataset
        if path.endswith(".csv"):
            path = path[:-4]

        # the path must be valid in resolved state
        path = self._resolve_path(path, "dummy_run_id")

        # TODO: currently we allow any characters in path
        # is_valid = re.match("^[\w_/]+$", path) is not None
        # return is_valid
        return True

    def _is_versioned_path(
        self,
        path: str,
        run_id: str,
    ):

        # path MUST be within the run folder
        # which can be either full DBFS path or blob path

        # the path must be valid in resolved state
        resolved_path = self._resolve_path(path, run_id)

        path_run_folder_blob = Path(PATH_RUN_FOLDERS).as_posix()
        path_run_folder_dbfs = Path(PATH_ROOT, path_run_folder_blob).as_posix()

        # check the the path contains a run id (any run id works here
        # because the user might use a hardcoded path from a different run)
        pat_1 = f"^" + path_run_folder_blob + "/" + RUN_ID_PATTERN
        pat_2 = f"^" + path_run_folder_dbfs + "/" + RUN_ID_PATTERN
        check_1 = re.match(pat_1, resolved_path) is not None
        check_2 = re.match(pat_2, resolved_path) is not None

        is_path_versioned_non_test = check_1 or check_2

        # similar check for test locations
        # check the the path contains a run id (any run id works here
        # because the user might use a hardcoded path from a different run)
        pat_1 = f"^" + path_run_folder_blob + "/test_" + RUN_ID_PATTERN
        pat_2 = f"^" + path_run_folder_dbfs + "/test_" + RUN_ID_PATTERN
        check_1 = re.match(pat_1, resolved_path) is not None
        check_2 = re.match(pat_2, resolved_path) is not None

        is_path_versioned_test = check_1 or check_2

        is_path_versioned = is_path_versioned_test or is_path_versioned_non_test

        return is_path_versioned

    def _flatten_data_conf(
        self,
        data_conf: Dict[str, Union[str, Dict[str, str]]],
    ):
        """
        "Flattens" a dictionary.
        Simply takes a nested dictionary and unfolds it to be flat
        (keys must be unique of course for this to happen properly)

        Parameters
        ----------
        data_conf: the dictionary to flatten

        Returns
        -------
        flat dictionary
        """

        list_paths = []

        for item in data_conf.values():
            if isinstance(item, dict):
                list_paths += list(item.values())
                continue

            list_paths += [item]

        return list_paths

    def get_data_config(self, config: Dict[str, Any], run_id: str):
        """
        validates how datasets are configured throughout Space Prod
        configs. Also used to generate the config but only data-specific paths
        """

        # make sure external config paths are part of the complete config
        # paths list
        keys_external = [".".join(x) for x in EXTERNAL_DATA_CONFIG_PATHS]
        keys_all = [".".join(x) for x in DATA_CONFIG_PATHS]
        msg = "Make sure EXTERNAL_DATA_CONFIG_PATHS are all in DATA_CONFIG_PATHS"
        assert len(set(keys_external) - set(keys_all)) == 0, msg

        conf_data = {}

        for address in DATA_CONFIG_PATHS:

            is_valid_addr = all([self._is_valid_conf_param(x) for x in address])
            msg = f"Address must contain alphanumeric characters and underscores: {address}"
            assert is_valid_addr, msg

            key = ".".join(address)
            msg = f"Duplicate address specified: {address}"
            assert key not in conf_data.keys(), msg

            # locate the list of dataset for this address
            this_conf_data = self.get_config_address_value(address, config)

            # make sure this dataset config section has unique paths
            # will ensure that we are not over-writing same path
            # while thinking its 2 different datasets
            # (this has been a real issue in the past)
            msg = f"""
            This config section has datasets with same physical paths.
            The pipeline will be overwriting itself.
            This is not allowed.
            See this config section path: '{key}'.  
            """

            this_conf_paths = self._flatten_data_conf(this_conf_data)
            check = len(set(this_conf_paths)) == len(this_conf_paths)
            assert check, msg

            # validate individual datasets in this section of config
            for dataset_id, dataset_path in this_conf_data.items():

                msg = f"""
                The following dataset is not valid:
                 - dataset ID location: '{key}'
                 - dataset ID: '{dataset_id}'
                 - dataset path: '{dataset_path}' 
                
                Please make sure  
                1. dataset ID only contains alphanumeric characters or underscores
                 
                2. dataset path only contains alphanumeric characters,
                   underscores or forward slashes  
                
                3. if the dataset is by region, make sure only the following 
                   regions are used: {REGIONS}
                """

                assert self._is_valid_dataset_id(dataset_id), msg
                assert self._is_valid_dataset_path(dataset_path), msg

                # make sure this dataset id is not used elsewhere (its unique)
                list_found = self._find_dataset_id(dataset_id, conf_data)
                list_found = list_found + [key]
                msg_found = "\n".join([f"- {x}" for x in list_found])

                msg = f"""
                The following dataset ID: '{dataset_id}'
                is not unique. It is found in more than one 
                dataset locations:\n{msg_found}
                """

                assert len(list_found) == 1, msg

                # make sure this dataset path is not used elsewhere (its unique)
                # will ensure that we are not over-writing same path
                # thinking its 2 different datasets
                # (this has been a real issue in the past)
                list_found = self._find_dataset_path(dataset_path, conf_data)
                list_found = list_found + [key]
                msg_found = "\n".join([f"- {x}" for x in list_found])

                msg = f"""
                The following dataset path: '{dataset_id}': '{dataset_path}'
                is not unique. It is found in more than one 
                dataset locations:\n{msg_found}
                """

                assert len(list_found) == 1, msg

                # make sure that the path is versioned, if this is not an
                # external path
                if key not in keys_external:
                    is_versioned = self._is_versioned_path(dataset_path, run_id)

                    # currently we don't allow writing data to paths that are
                    # not versioned by run ID
                    msg = f"""
                    The following dataset ID: '{dataset_id}' has been resolved
                    to a path that is not versioned AND/OR it is outside of 
                    the current run folder, currently we don't allow writing 
                    to un-versioned paths or writing outside of the run folder. 
                    Please make sure your path in YAML starts with 
                        '{PATH_RUN_FOLDERS}/{{run_id}}/'
                        OR 
                        '{PATH_ROOT}/{PATH_RUN_FOLDERS}/{{run_id}}/'        
                    
                    Current dataset path: {dataset_path}
                    Location of this dataset in the config: {key}
                    """

                    assert is_versioned, msg

            conf_data[key] = this_conf_data

        return conf_data
