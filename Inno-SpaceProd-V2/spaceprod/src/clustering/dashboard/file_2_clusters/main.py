from typing import Any, Dict

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from spaceprod.src.clustering.dashboard.file_2_clusters.helpers import (
    run_internal_clustering_section_dashboard,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark


@timeit
def task_run_internal_clustering_section_dashboard():
    df_internal_seg_profile_section = context.data.read("section")
    df_section_sale_summary = context.data.read("sale_summary_section")

    df_internal_seg_profile_section = df_internal_seg_profile_section.toPandas()
    df_sales = df_section_sale_summary.toPandas()

    pdf_final = run_internal_clustering_section_dashboard(
        df_internal_seg_profile_section=df_internal_seg_profile_section,
        df_sales=df_sales,
    )

    context.data.write_csv("file_2_view", spark.createDataFrame(pdf_final))
