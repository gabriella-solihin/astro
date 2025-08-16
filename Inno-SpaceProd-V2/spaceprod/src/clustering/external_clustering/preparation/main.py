from functools import reduce
from operator import and_
from typing import Any, Dict

import pandas as pd

from pyspark.sql import SparkSession
from spaceprod.src.clustering.external_clustering.preparation.helpers import (
    clean_location,
    final_patch_to_environics_data,
    preprocess_competitors,
    preprocess_demographics,
    process_store_data,
    read_format_data_header,
    combine_dataframe,
)
from spaceprod.utils.data_helpers import read_blob, read_blob_csv, write_blob
from spaceprod.utils.data_transformation import is_col_null_mask
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.validation import dup_check

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 12)
pd.set_option("max_columns", None)
pd.set_option("expand_frame_repr", None)


@timeit
def task_data_preparation_environics():
    """function to prepare input data for external clustering

    prepare input data for external clustering including:
    1. merge demographic data within 5, 10, and 15 mins drive
    2. merge competitor data
    3. join with the location data based on Banner Key

    Parameters
    ----------
    spark : SparkSession
        spark session for databricks cluster
    config: Dict
        configuration including parameters
    Returns
    -------
    pd.DataFrame
        prepared data for external clustering

    """

    # determine which config(s) we need for this task
    config = context.config["clustering"]["external_clustering_config"]

    ###########################################################################
    # GET CONFIG PARAMS
    ###########################################################################

    # Environics data column prefix which is read from external clustering config
    col_name_prefix = config["column"]["prefix"]

    # Banner key to be updated with below banner corrected format
    banner_corrected_key = "1016225"

    # Banner format for the given banner corrected key
    banner_corrected_format = "Discount"

    # List of attributes for join operations in the cleaning of competitor data
    competitor_group = ["DRIVE_TIME", "BK"]

    # Details of the store to be corrected
    filter_store_corrected = {
        "STORE_NO": "4761",
        "STORE_PHYSICAL_LOCATION_NO": "11429",
        "NATIONAL_BANNER_DESC": "SOBEYS",
        "REGION_DESC": "Ontario",
        "POSTAL_CD": "XXXXXX",
    }

    ###########################################################################
    # MISSING STORES
    ###########################################################################

    # Dataset with missing store list based on postal code (need to config and refactor)
    missing_store = {
        "G0A4J0": "6124",
        "L0K1B0": "3308",
        "L7C1K2": "3303",
        "L0K1E0": "3302",
        "L4M4Y8": "3310",
        "A0K2E0": "9218",
        "N3B2P5": "3301",
        "L0G1M0": "3279",
        "M2J1L8": "3297",
        "L0G1T0": "3276",
        "N4X1A6": "3296",
        "P4N7C3": "3286",
        "N0G2W0": "3263",
        "N4S1E4": "3291",
        "L8L3B4": "3884",
        "L6T3S1": "3857",
        "L2Y2A6": "3859",
        "L1M1T6": "3861",
        "L2H0L4": "3811",
        "L1C3A0": "3871",
        "L1V1A6": "3865",
        "L1C0C7": "3892",
        "L1G4W7": "3870",
        "N2A2Y2": "3882",
        "J9H6H4": "8052",
        "H4C1R1": "8063",
        "J0P1P0": "8071",
        "H3K1P2": "8066",
        "J2W2A3": "8164",
        "J3L4Y3": "6964",
        "J5R1W7": "8031",
        "J5R0B4": "8032",
        "J2W1C7": "8163",
        "J6A2P5": "8058",
        "S0N1H0": "3113",
        "R2V1Y9": "5267",
        "J4Y1A1": "4001",
        "H3S1J1": "4002",
        "H1Z2G1": "4004",
        "H4K2V6": "4005",
        "B4A3Y4": "10061",
        "B3J4B1": "10060",
        "N0P1A0": "4107",
        "N7M1W3": "4116",
        "N2Z3B9": "4108",
        "G0R1H0": "8702",
        "G0J3K0": "8795",
        "A0K1B0": "9282",
        "J3L3R8": "6964",
        "J0J2A0": "8164",
        "N1M2W3": "3862",
        "T8N5J9": "5067",
        "T6J4V9": "8957",
        "L9T3H5": "3863",
        "T0M0J0": "9515",
        "A8A1E6": "9218",
    }

    # Dataset containing ban nos of duplicated stores in same postal code (need to config and refactor)
    duplicated_store = {
        "5621": "5621",
        "886": "886",
        "7267": "7267",
        "66": "66",
        "3011": "3011",
        "609": "609",
        "5118": "5118",
        "7583": "7583",
        "7020": "7020",
        "7506": "7506",
        "818": "818",
        "70019": "70019",
        "70044": "70044",
        "70017": "70017",
        "7265": "7265",
        "70041": "70041",
        "70039": "70039",
        "70038": "70038",
        "70034": "70034",
    }

    ###########################################################################
    # LOAD MISSING STORE DATA
    ###########################################################################

    missing_ban_no = pd.DataFrame(
        missing_store.items(), columns=["POSTAL_CD", "BAN_NO_CORRECTED"]
    )
    update_missing_ban_no = pd.DataFrame(
        duplicated_store.items(), columns=["BAN_NO", "BAN_NO_FINAL"]
    )

    ###########################################################################
    # LOAD INPUT DATA
    ###########################################################################

    # read the location data
    df_location = context.data.read("location")

    # read the store data
    pdf_store_list = context.data.read("store_list").toPandas()

    # read the demographics within 5min distance of each store
    pdf_demographics_data_5_min = context.data.read(
        "demographics_5mins_drive"
    ).toPandas()

    # read the demographics within 10min distance of each store
    pdf_demographics_data_10_min = context.data.read(
        "demographics_10mins_drive"
    ).toPandas()

    # read the demographics within 15min distance of each store
    pdf_demographics_data_15_min = context.data.read(
        "demographics_15mins_drive"
    ).toPandas()

    # read the competitor data
    pdf_competitor_data = context.data.read("competitors").toPandas()

    ###########################################################################
    # PROCESS DATA
    ###########################################################################

    # Below function calls select/drop columns, cleans the header information and replaces the unwanted word with 0.

    df_location = clean_location(df_loc=df_location)

    pdf_demographics_data_5_min = read_format_data_header(
        df=pdf_demographics_data_5_min,
        isdemographic=True,
        prefix=col_name_prefix,
    )

    pdf_demographics_data_10_min = read_format_data_header(
        df=pdf_demographics_data_10_min,
        isdemographic=True,
        prefix=col_name_prefix,
    )

    pdf_demographics_data_15_min = read_format_data_header(
        df=pdf_demographics_data_15_min,
        isdemographic=True,
        prefix=col_name_prefix,
    )

    pdf_store_list = read_format_data_header(
        df=pdf_store_list,
        isdemographic=False,
        prefix=col_name_prefix,
    )

    pdf_competitor_data = read_format_data_header(
        df=pdf_competitor_data,
        isdemographic=False,
        prefix=col_name_prefix,
    )

    demographic_dfs = combine_dataframe(
        pdf_demographics_data_5_min,
        pdf_demographics_data_10_min,
        pdf_demographics_data_15_min,
    )

    # This performs ETL operation on the competitors data
    pdf_competitor_processed = preprocess_competitors(
        competitor_data_df=pdf_competitor_data,
        competitor_group=competitor_group,
        banner_corrected_key=banner_corrected_key,
        banner_corrected_format=banner_corrected_format,
    )

    # This performs ETL operation on the demographic data and returns a combined list of demographic data
    pdf_demographic_processed = preprocess_demographics(
        dfs=demographic_dfs,
        dist_list=["_5", "_10", "_15"],
        col_prefix=col_name_prefix,
        key="BANNER_KEY",
    )

    # This performs ETL operation on the store list environic data data and returns a combined df of stores and
    # location data.
    pdf_store_list_processed = process_store_data(
        store_list_df=pdf_store_list,
        location_df=df_location,
        filter_store_corrected=filter_store_corrected,
        missing_ban_no=missing_ban_no,
        update_missing_ban_no=update_missing_ban_no,
    )

    # TODO: @ANIL, to investigate why duplicates are coming up in this dataset
    pdf_store_list_processed = pdf_store_list_processed.drop_duplicates(
        subset=["BANNER_KEY"]
    )

    # This step combines 3 given dataframes
    l_dfs = combine_dataframe(
        pdf_demographic_processed,
        pdf_competitor_processed,
        pdf_store_list_processed,
    )

    dfs = [d.set_index("BANNER_KEY") for d in l_dfs]

    pdf = pd.concat(objs=dfs, axis=1, join="inner").reset_index()

    df_output = spark.createDataFrame(pdf)

    # TODO: to be addressed
    df_output = final_patch_to_environics_data(
        df=df_output,
        col_name_prefix=col_name_prefix,
    )

    # Dropping an index column However commenting it as its not adopted in the downstream
    # df_output = df_output.drop("index")

    context.data.write(
        dataset_id="environics_output",
        df=df_output,
    )

    # final dup check
    dup_check(context.data.read("environics_output"), ["BANNER_KEY"])
