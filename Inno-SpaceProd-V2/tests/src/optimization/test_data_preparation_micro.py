import pytest
import os
from pathlib import Path

from spaceprod.utils.config_helpers import parse_config


@pytest.fixture(scope="session")
def config():
    current_file_path = Path(__file__).resolve()

    config_test_path = os.path.join(current_file_path.parents[3], "tests", "config")
    global_config_path = os.path.join(config_test_path, "spaceprod_config.yaml")

    config_path = os.path.join(
        config_test_path, "optimization", "micro_optimization_config.yaml"
    )

    config = parse_config([global_config_path, config_path])
    config["pipeline_steps"]["optimization"]["micro"] = True
    return config


@pytest.mark.skip  # TODO: address: https://dev.azure.com/SobeysInc/Inno-SpaceProd/_TestManagement/Runs?runId=108442
def test_process_all_micro_raw_data(spark, config):

    data = process_all_micro_raw_data(config)
    assert type(data[0]).__name__ == "tuple"
    assert data[0][1].startswith("CANNIB")
