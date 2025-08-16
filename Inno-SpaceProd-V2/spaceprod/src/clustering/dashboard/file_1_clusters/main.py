from typing import Any, Dict

from pyspark.sql import SparkSession
from spaceprod.src.clustering.dashboard.file_1_clusters.helpers import (
    log_summary,
    pre_process_mission_summary_data,
    pre_process_store_list_data,
    produce_file_1_view,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark


@timeit
def task_file_1_clusters():
    ###########################################################################
    # READ IN REQUIRED DATA
    ###########################################################################

    df_location = context.data.read("location")
    df_mission_summ = context.data.read("mission_summary")
    df_merge_clusters = context.data.read("merged_clusters")
    df_store_list = context.data.read("store_list")

    ###########################################################################
    # CALL REQUIRED HELPER FUNCTIONS TO PROCESS DATA
    ###########################################################################

    df_mission_summ_processed = pre_process_mission_summary_data(
        df_mission_summ=df_mission_summ,
        df_location=df_location,
    )

    df_store_list_processed = pre_process_store_list_data(
        df_store_list=df_store_list,
    )

    df_file_1_view = produce_file_1_view(
        df_merge_clusters=df_merge_clusters,
        df_store_list_processed=df_store_list_processed,
        df_mission_summ_processed=df_mission_summ_processed,
    )

    ###########################################################################
    # SAVE RESULTS
    ###########################################################################

    context.data.write("file_1_view_parquet", df_file_1_view)
    context.data.write_csv("file_1_view", df_file_1_view)

    # log summary information
    log_summary(context.data.read("file_1_view_parquet"))
