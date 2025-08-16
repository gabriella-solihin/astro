"""This module includes utils functions to help work with
yaml configuration files."""

from typing import List, Dict, Any, Union
import os
import yaml
from dotenv import load_dotenv
from pathlib import Path
from cerberus import Validator

from inno_utils.loggers import log


class ConfigException(Exception):
    """
    Exception for configuration error
    """

    pass


def read_yml(yml_path: str = "") -> Dict[str, Any]:
    """
    Load the contents that are contained in a yaml config file

    Parameters
    ----------
    yml_path
        The string path of the yaml file to be read

    Returns
    -------
    config: Dict[str, Any]
        The contents of the yaml file as a dictionary
    """
    yml_path = os.path.normpath(yml_path)

    with open(yml_path, "r") as file:
        try:
            # load the corresponding yaml file
            config = yaml.load(file, Loader=yaml.SafeLoader)
            return config
        except ValueError:
            err_msg = (
                f"could not part the yaml file in the following path:\n" f"{yml_path}"
            )
            raise ConfigException(err_msg)


def build_validator(path: Path):
    """builds validation
    Parameters
    --------
    path: path
    """
    schema = eval(open(path, "r").read())
    return Validator(schema)


def parse_config(config_files: Union[str, Path, List[Union[str, Path]]]) -> dict:
    """
    Parse the configuration based on the list of configuration files.

    Parameters
    ----------
    config_files
        Config file or list of config files to be parsed
    Returns
    --------
    config: dict
        configuration
    """

    if not isinstance(config_files, list):
        config_files = [config_files]

    # create a blank dictionary to keep the info of all the loaded configs
    config: Dict[str, Any] = {}

    # loop through each config and append to the config dictionary
    for config_file in config_files:

        # convert to string if it's a Path object
        if isinstance(config_file, Path):
            config_file = str(config_file)

        # load the config
        curr_config = read_yml(config_file)

        # validate the config if a schema file exists
        config_path = Path(config_file)
        schema_path = Path(config_path.parent, config_path.stem + "_schema.py")
        if schema_path.exists():
            validator = build_validator(schema_path)
            if not validator.validate(curr_config):
                log.warning(f"Supplied config {config_file} does not match its schema.")
                log.warning(validator.errors)

        # update the main config dictionary
        config.update(curr_config)

    return config


def read_env(env_path: str = "") -> None:
    """
    Loads the environment variables from ``.env`` file.

    Parameters
    ----------
    env_path
        The path where the ``.env`` file is located

    Returns
    -------
    :
        None
    """
    env_path = os.path.normpath(env_path)
    # load the corresponding env file
    load_dotenv(env_path)
