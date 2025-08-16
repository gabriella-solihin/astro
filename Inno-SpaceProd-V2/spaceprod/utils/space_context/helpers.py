import collections.abc
import os
from pathlib import Path
from typing import List, Dict, Any

from inno_utils.loggers import log

from spaceprod.utils.config_helpers import read_yml


def get_root_dir():
    """finds the root path of the repo"""
    import spaceprod

    return Path(spaceprod.__file__).parent.as_posix()


def get_config_dir():
    """finds the root path in which configs live in the repo"""
    list_elements = [
        Path(get_root_dir()).parent,
        "pipeline",
        "config",
    ]

    return Path(*list_elements).as_posix()


def get_config_file_list() -> List[str]:
    """lists all objects inside the config root path in the repo"""

    # start with the config folder
    path_conf = get_config_dir()

    # first list all the "modules", i.e. folders in the config folder
    list_paths_modules = [
        x
        for x in os.listdir(path_conf)
        if os.path.isdir(os.path.join(path_conf, x)) and x != "__pycache__"
    ]

    config_list = []

    # inside "modules" get module-specific configs (yml files)
    for module_name in list_paths_modules:
        module_path = os.path.join(path_conf, module_name)
        module_configs = [
            os.path.join(module_path, x)
            for x in os.listdir(module_path)
            if x.endswith(".yaml")
        ]
        config_list += module_configs

    # get general configs
    list_paths_general_confs = [
        os.path.join(path_conf, x) for x in os.listdir(path_conf) if x.endswith(".yaml")
    ]

    # combine everything together to get the complete list
    config_list = config_list + list_paths_general_confs

    return config_list


def read_config() -> Dict[str, Dict[str, Any]]:
    """
    Recursively reads and parses Space Prod configs to construct a
    single Python dictionary config to be used throughout modules
    """

    list_config = get_config_file_list()
    assert len(set(list_config)) == len(list_config), "Dup config paths found"
    config_dir = get_config_dir()

    dict_config = {}

    for path in list_config:
        path_posix = Path(path).as_posix()

        msg = f"This config: '{path_posix}' is not in config dir: {config_dir}"
        assert config_dir in path_posix, msg

        # determine the sub-path to config file, it will become  part of the
        # config dictionary to ensure
        segments = Path(path_posix).parts[len(Path(config_dir).parts) :]
        segments = [os.path.splitext(x)[0] for x in segments]

        # load the config and add it as the final segment
        config_contents = read_yml(path_posix)

        # glue segments and the config contents together
        sub_conf = config_contents
        for seg in reversed(segments):
            sub_conf = {seg: sub_conf}

        # append to the final config
        dict_config = merge_dicts(dict_config, sub_conf)

    return dict_config


def merge_dicts(old: Dict[Any, Any], new: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    Merge the two dictionaries together into a single dictionary.
    Priority will go to the ``new`` dictionary object when the same
    key exists in both of the dictionaries.

    Parameters
    ----------
    old:
        Dictionary to merge the new values into

    new:
        Dictionary to merge into the old dictionary

    Returns
    -------
    :
        Merged dictionary containing values from both of the dictionaries
    """
    for k, v in new.items():
        if isinstance(old, collections.abc.Mapping):
            if isinstance(v, collections.abc.Mapping):
                old[k] = merge_dicts(old.get(k, {}), v)
            else:
                old[k] = v
        else:
            old = {k: v}

    return old
