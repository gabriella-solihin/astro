import os
import re
from typing import Any, Dict

import pytest

from spaceprod.utils.space_context.data_config_validator import DATA_CONFIG_PATHS


@pytest.fixture(scope="session")
def setup_settings():
    from unittest import mock
    import setuptools

    with mock.patch.object(setuptools, "setup") as mock_setup:
        import setup  # This is setup.py which calls setuptools.setup

    # returns args, kwargs and kwargs here is the information from the setup
    _, setup_settings = mock_setup.call_args

    return setup_settings


def test_package_data(setup_settings: Dict[str, Any]):

    # get the package data list from setup.py
    package_data = setup_settings["package_data"]["pipeline"]

    # get config path, and make sure it exists
    import spaceprod

    path_package = spaceprod.__path__[0]
    path_repo = os.path.dirname(path_package)
    path_conf = os.path.join(path_repo, "pipeline", "config")
    msg = f"Conf path must exist here: {path_conf}"
    assert os.path.isdir(path_conf), msg

    # get contents of conf path
    list_conf = os.listdir(path_conf)

    # we always want to include all configs in the root config folder
    root_config_ymls = "config/*.yaml"
    msg = f"You are not including YMLs in the root: {root_config_ymls}"
    assert root_config_ymls in package_data, msg

    for i in DATA_CONFIG_PATHS:
        # first element must be either a file or a folder name
        # if it is a folder, it is explicitly stated in 'package_data'
        # if it is a file, it is already captured by 'config/*.yaml'
        elem = i[0]
        file_name = elem + ".yaml"

        # if it is a file, it is already captured by 'config/*.yaml'
        if file_name in list_conf:
            continue

        # it than must be a directory in the config folder if it found
        # in the DATA_CONFIG_PATHS
        fold_path = os.path.join(path_conf, elem)
        msg = f"Must be a directory: {fold_path}"
        assert os.path.isdir(fold_path), msg

        # given that it is a dir, we expect to have this pattern in the
        # setup.py's 'package_data'
        pattern = f"config/{elem}/*.yaml"

        msg = f"""
        There is a new config that is not added to setup.py's 'package_data'
        Probably need to add: '{pattern}'
        """

        assert pattern in package_data, msg


def test_versioned_pinned(setup_settings: Dict[str, Any]):

    # check that each dependency has a version pinned
    for dep in setup_settings["install_requires"]:

        # skip if it is a reference to another Inno repo
        if " @ git+" in dep:
            continue

        msg = f"Looks like dependency does have a version pinned: {dep}"
        assert "==" in dep, msg

        # dependency version should contain only numbers and dots
        pattern = "^\d+(\.\d+)*$"
        ver = dep.split("==")[1]
        is_valid = re.match(pattern, ver) is not None
        assert is_valid, msg
