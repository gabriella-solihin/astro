import os
from typing import Optional

from inno_utils.loggers import log


def get_spark_for_space(use_test_spark_conf: Optional[bool] = False):
    """
    Wrapper around custom configuration for Space spark
    TODO: this standard configuration can change in future
    """

    from pyspark.sql.session import SparkSession

    is_env = os.getenv("SPACE_USE_TEST_SPARK_CONF") is not None
    is_arg = use_test_spark_conf

    if is_env or is_arg:

        log.info("Setting spark conf for Tests")

        spark = (
            SparkSession.builder.appName("Space Prod Tests")
            .master("local[*]")
            .config("spark.default.parallelism", "10")
            .config("spark.sql.shuffle.partitions", "10")
            .config("spark.sql.autoBroadcastJoinThreshold", "-1")
            .config("spark.databricks.service.server.enabled", "true")
            # .config("spark.cleaner.ttl", "60")
            # .config("spark.submit.deployMode", "client")
            .getOrCreate()
        )

    else:
        log.info("Using default spark conf")

        spark = SparkSession.builder.appName("Space Prod").getOrCreate()

    # build the wheel locally and add the path to it sparkContext
    # this is needed for successful runs for UDFs so that code is
    # available to workers
    var = "SPACE_ADD_WHEEL_TO_SPARK"

    if os.getenv(var) is not None:

        msg = f"""
        Adding wheel to spark!
        To avoid this step remove the '{var}' env var. 
        If you are avoiding this step, make sure you are not using UDFs or 
        your cluster already has latest wheel installed.
        """

        log.info(msg)

        from spaceprod.utils.deployment.wheel import build_whl_locally

        path_wheel = build_whl_locally()
        spark.sparkContext.addFile(path_wheel)

    else:

        log.info("Not building and attaching wheel to Spark")

    return spark


class SparkSpaceProd(object):
    _spark = None

    def __new__(cls, *args, **kwargs):
        if not cls._spark:
            cls._spark = get_spark_for_space()

        return cls._spark


spark = SparkSpaceProd()
