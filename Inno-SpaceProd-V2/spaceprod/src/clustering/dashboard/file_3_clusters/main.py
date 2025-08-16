from typing import Any, Dict

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from spaceprod.src.clustering.dashboard.file_3_clusters.helpers import (
    run_internal_cluster_need_state_dashboard,
)
from spaceprod.utils.decorators import timeit

from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark


@timeit
def task_run_internal_cluster_need_state_dashboard():
    # same as run_internal_clustering_section_dashboard but on need state level
    # mostly same but different files and now on need state level as well
    # also pulling in item name for top need state item so that we can name the NS
    df_internal_seg_profile_section_ns = context.data.read("section_ns")
    df_section_ns_sale_summary = context.data.read("sale_summary_need_state")
    need_states = context.data.read("final_need_states")

    df_internal_seg_profile_section = df_internal_seg_profile_section_ns.toPandas()
    df_sales = df_section_ns_sale_summary.toPandas()

    pdf_final = run_internal_cluster_need_state_dashboard(
        df_internal_seg_profile_section=df_internal_seg_profile_section,
        df_sales=df_sales,
        need_states=need_states,
    )

    context.data.write_csv("file_3_view", spark.createDataFrame(pdf_final))
