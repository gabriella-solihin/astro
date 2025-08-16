from typing import Any, Dict

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from spaceprod.src.clustering.dashboard.file_5_clusters.helpers import (
    run_external_cluster_competition_dashboard,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark


@timeit
def task_run_external_cluster_competition_dashboard():
    """this file focuses on the index and st.dev. of external clustering, competitors only"""
    df = context.data.read("profiling").toPandas()
    dashboard_names = context.data.read("competitor_dashboard_names").toPandas()

    pdf = run_external_cluster_competition_dashboard(
        pdf=df, pdf_dashboard_names=dashboard_names
    )

    context.data.write_csv("file_5_view", spark.createDataFrame(pdf))
