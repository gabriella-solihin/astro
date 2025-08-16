import os

from unittest import mock
from pathlib import Path

import pytest

from spaceprod.utils.config_helpers import read_yml, parse_config


@mock.patch("spaceprod.utils.config_helpers.yaml.load")
def test_read_yml(mock_obj):
    yml_path = "/path/to/global_config.yml"

    with mock.patch("builtins.open", mock.mock_open(read_data="data")) as mock_file:
        read_yml(yml_path)
        assert open(yml_path).read() == "data"
        assert mock_obj.call_count == 1


@pytest.mark.skip  # TODO: address: https://dev.azure.com/SobeysInc/Inno-SpaceProd/_TestManagement/Runs?runId=108442
def test_parse_config():
    current_file_path = Path(__file__).resolve()

    config_test_path = os.path.join(current_file_path.parents[2], "tests", "config")
    global_config_path = os.path.join(config_test_path, "spaceprod_config.yaml")

    config_dict = parse_config([global_config_path])

    assert config_dict["output_dir"] == "sobeys_space_prod/dev/"
    assert config_dict["environment"] == "testing"
