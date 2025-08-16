from typing import Any, Dict

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from spaceprod.src.clustering.dashboard.file_4_clusters.helpers import (
    run_external_cluster_feature_dashboard,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark


@timeit
def task_run_external_cluster_feature_dashboard():
    """this file focuses on the index and st.dev. of external clustering, demographics only"""

    pdf = context.data.read("profiling").toPandas()
    dashboard_names = context.data.read("feature_dashboard_names").toPandas()

    pdf = run_external_cluster_feature_dashboard(
        df=pdf, dashboard_names=dashboard_names
    )

    context.data.write_csv("file_4_view", spark.createDataFrame(pdf))
