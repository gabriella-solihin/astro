from typing import Any, Dict

from pyspark.sql import SparkSession
from spaceprod.src.clustering.dashboard.shopping_mission_summary.smis_model_profiler import (
    SmisModelProfiler,
)
from spaceprod.src.clustering.dashboard.shopping_mission_summary.transaction_processing import (
    process_transaction_data,
)
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark


@timeit
def task_shopping_mission_summary():

    ###########################################################################
    # ACCESS THE REQUIRED CONFIG PARAMETERS
    ###########################################################################
    config_scope = context.config["scope"]

    regions = config_scope["regions"]
    banners = config_scope["banners"]
    txn_start_date = config_scope["st_date"]
    txn_end_date = config_scope["end_date"]
    lvl2_name_exclusions = []

    ###########################################################################
    # READ IN REQUIRED DATA
    ###########################################################################

    df_product = context.data.read("product")
    df_location = context.data.read("location")
    df_txnitem = context.data.read("txnitem", regions=regions)

    ###########################################################################
    # CALL REQUIRED HELPER FUNCTIONS TO PROCESS DATA
    ###########################################################################

    # Create the profiling object
    df_trans_processed = process_transaction_data(
        df_product=df_product,
        df_location=df_location,
        df_txnitem=df_txnitem,
        lvl2_name_exclusions=lvl2_name_exclusions,
        banners=banners,
        txn_start_date=txn_start_date,
        txn_end_date=txn_end_date,
    )

    profiler = SmisModelProfiler(df=df_trans_processed)

    df_mission_summary = profiler.create_mission_summ(cluster_label=["STORE_NO"])

    ###########################################################################
    # SAVE RESULTS
    ###########################################################################

    context.data.write("mission_summary", df_mission_summary)
