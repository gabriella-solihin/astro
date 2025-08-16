import sys
import os
import pytest
from pyspark.sql import SparkSession

from spaceprod.utils.config_helpers import read_env


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    from tests.utils.spark_util import spark_tests as spark

    return spark


# Make sure that the application source directory (this directory's parent) is
# on sys.path.
here = os.path.normpath(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(here)
sys.path.insert(0, here)
env_path = os.path.join(here, "tests", ".env")
read_env(env_path)
