import pytest
import os
from pathlib import Path


from spaceprod.src.elasticity.model_run.modeling import call_bay_model
from spaceprod.utils.config_helpers import parse_config


from spaceprod.utils.names import get_col_names

n = get_col_names()


@pytest.fixture(scope="session")
def config():
    current_file_path = Path(__file__).resolve()

    config_test_path = os.path.join(current_file_path.parents[3], "tests", "config")
    global_config_path = os.path.join(config_test_path, "spaceprod_config.yaml")

    config_path = os.path.join(
        config_test_path, "elasticity", "micro_elasticity_config.yaml"
    )

    config = parse_config([global_config_path, config_path])
    config["pipeline_steps"]["elasticity"]["micro"] = True
    return config


@pytest.fixture(scope="session")
def fit(bay_data_df, spark, config):
    fit = call_bay_model(bay_data_df, config, write=False)
    return fit
