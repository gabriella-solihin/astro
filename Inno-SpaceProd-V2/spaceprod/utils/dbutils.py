import traceback

from inno_utils.loggers import log


def get_dbutils():
    """Get the Databricks utility library ``dbutils``. Note that this
    assumes you are running on Databricks runtime

    Returns
    -------
    :
        The ``dbutils`` library if running in Databricks

    Raises
    ------
    ImportError
        If ``dbutils`` could not be imported

    Notes
    -----
    This snippet of code is inspired by the official Databricks documentation
    on accessing dbutils.
    https://docs.databricks.com/dev-tools/databricks-connect.html#access-dbutils
    """
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        from spaceprod.utils.space_context.spark import spark

        dbutils = DBUtils(spark)
    except Exception:

        tb = traceback.format_exc()
        log.info(f"WARNING! DBUTILS: getting error:\n{tb}\n")

        try:
            import IPython

            ipython = IPython.get_ipython()

            if ipython is None:
                log.info(f"WARNING! Could not generate iPython. No DButils")
                return

            dbutils = ipython.user_ns["dbutils"]

        except Exception:
            tb = traceback.format_exc()
            log.info(f"WARNING! DBUTILS: getting error:\n{tb}\n")

            return

    return dbutils
