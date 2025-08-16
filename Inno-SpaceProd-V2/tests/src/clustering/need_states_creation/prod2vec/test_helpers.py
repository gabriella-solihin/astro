import traceback

import pytest

from inno_utils.loggers.log import log
from pyspark import Row
from pyspark.sql import SparkSession
from spaceprod.src.clustering.need_states_creation.prod2vec.helpers import (
    generate_execution_id,
    limit_modeling_scope,
    log_pairs_summary,
    pre_process_data_for_prod2vec,
    select_staged_data_columns_and_rows,
    validate_prod2vec_output,
    create_exec_id_run_dimensions_lookup,
    pivot_result,
    lookup_region_banner_pog,
    log_entity_counts_staged_pairs,
    log_entity_counts_prod2vec,
)
from spaceprod.utils.imports import F


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    from tests.utils.spark_util import spark_tests as spark

    return spark


def test_generate_execution_id(spark: SparkSession):
    df = spark.createDataFrame(
        [
            Row(
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Some Section Master !?",
            )
        ]
    )

    df_res = generate_execution_id(df)

    cols = df.columns + ["EXEC_ID"]
    assert set(cols) == set(df_res.columns)

    val = "ontario_SOBEYS_SomeSectionMaster"
    assert df_res.select("EXEC_ID").collect()[0]["EXEC_ID"] == val


def test_log_pairs_summary(spark: SparkSession):

    df = spark.createDataFrame(
        [
            Row(
                ITEM_A="1",
                ITEM_B="2",
                EXEC_ID="EXEC_ID_A",
            )
        ]
    )

    log_pairs_summary(df)

    # TODO: assert that correct result is being logged
    #  (need to capture stdout from logging)


def test_pre_process_data_for_prod2vec(spark: SparkSession):

    df = spark.createDataFrame(
        [
            Row(
                ITEM_A="1",
                ITEM_B="2",
                EXEC_ID="EXEC_ID_1",
                SECTION_MASTER="SM_1",
                CUSTOMER_CARD_ID="CUST_1",
            ),
        ]
    )

    df_res = pre_process_data_for_prod2vec(df_cust_item_diff=df)
    cols_expected = df.columns + ["ITEM_COUNT"]
    df_expected = df.withColumn("ITEM_COUNT", F.lit(1)).select(*cols_expected)

    assert df_expected.exceptAll(df_res.select(*cols_expected)).limit(1).count() == 0

    # try with redundant pairs

    df = spark.createDataFrame(
        [
            Row(
                ITEM_A="1",
                ITEM_B="2",
                EXEC_ID="EXEC_ID_1",
                SECTION_MASTER="SM_1",
                CUSTOMER_CARD_ID="CUST_1",
            ),
            Row(
                ITEM_A="2",
                ITEM_B="1",
                EXEC_ID="EXEC_ID_1",
                SECTION_MASTER="SM_1",
                CUSTOMER_CARD_ID="CUST_1",
            ),
        ]
    )

    df_res = pre_process_data_for_prod2vec(df_cust_item_diff=df)
    assert df_res.count() == 1


def test_limit_modeling_scope(spark: SparkSession):

    df = spark.createDataFrame(
        [
            Row(EXEC_ID="EXEC_ID_1", ITEM_COUNT=1),
            Row(EXEC_ID="EXEC_ID_2", ITEM_COUNT=2),
        ]
    )

    df_res = limit_modeling_scope(
        spark=spark,
        df_inputs=df,
        item_limit_threshold=2,
        list_hard_excluded_exec_ids=[],
    )

    cols = [
        F.col("EXEC_ID"),
        F.lit(1).alias("REASON_THRESHOLD"),
        F.lit(None).alias("REASON_CONFIG"),
    ]

    df_expected = df.filter(F.col("EXEC_ID") == "EXEC_ID_1").select(*cols)

    assert df_res.exceptAll(df_expected).limit(1).count() == 0

    # try with hard exclusion list

    df_res = limit_modeling_scope(
        spark=spark,
        df_inputs=df,
        item_limit_threshold=2,
        list_hard_excluded_exec_ids=["EXEC_ID_2"],
    )

    mask = F.col("EXEC_ID") == "EXEC_ID_1"

    cols = [
        F.col("EXEC_ID"),
        F.when(mask, F.lit(1)).otherwise(F.lit(None)).alias("REASON_THRESHOLD"),
        F.when(mask, F.lit(None)).otherwise(F.lit(1)).alias("REASON_CONFIG"),
    ]

    df_expected = df.select(*cols)

    assert df_res.exceptAll(df_expected).limit(1).count() == 0


def test_validate_prod2vec_output(spark: SparkSession):

    df = spark.createDataFrame(
        [
            Row(EXEC_ID="EXEC_ID_1", EMBEDD_NAME="good"),
            Row(EXEC_ID="EXEC_ID_2", EMBEDD_NAME="EXCEPTION: bad"),
        ]
    )

    broke = False

    try:
        validate_prod2vec_output(df)
    except Exception:
        tb = traceback.format_exc()
        log.info(f"Exception was raised:\n{tb}\n")
        broke = True

    assert broke, "should break here"

    # here it should not break:
    validate_prod2vec_output(df.filter(F.col("EXEC_ID") == "EXEC_ID_1"))


def test_select_staged_data_columns_and_rows(spark: SparkSession):

    df = spark.createDataFrame(
        [
            Row(
                ITEM_A="1",
                ITEM_B="2",
                EXEC_ID="EXEC_ID_1",
                CUSTOMER_CARD_ID="CUST_1",
                ITEM_NO="1",
            ),
            Row(
                ITEM_A="2",
                ITEM_B="1",
                EXEC_ID="EXEC_ID_1",
                CUSTOMER_CARD_ID="CUST_1",
                ITEM_NO="2",
            ),
            Row(
                ITEM_A="3",
                ITEM_B="1",
                EXEC_ID="EXEC_ID_3",
                CUSTOMER_CARD_ID="CUST_2",
                ITEM_NO="3",
            ),
        ]
    )

    df_exclusions = spark.createDataFrame(
        [
            Row(
                EXEC_ID="EXEC_ID_1",
                REASON_CONFIG=1,
                REASON_THRESHOLD=None,
            ),
            Row(
                EXEC_ID="EXEC_ID_2",
                REASON_CONFIG=None,
                REASON_THRESHOLD=1,
            ),
        ]
    )

    df_result = select_staged_data_columns_and_rows(
        df_prod2vec_limit=df,
        df_exclusions=df_exclusions,
    )

    cols = ["EXEC_ID", "CUSTOMER_CARD_ID", "ITEM_A", "ITEM_B"]
    df_expected = df.filter(F.col("EXEC_ID") == "EXEC_ID_3").select(*cols)
    assert df_result.select(*cols).exceptAll(df_expected).limit(1).count() == 0


def test_create_exec_id_run_dimensions_lookup(spark: SparkSession):

    df = spark.createDataFrame(
        [
            Row(
                EXEC_ID="EXEC_ID_1",
                REGION="REGION",
                NATIONAL_BANNER_DESC="NATIONAL_BANNER_DESC",
                SECTION_MASTER="SECTION_MASTER",
                SOME_COLUMN="Some value",
            ),
        ]
    )

    df_res = create_exec_id_run_dimensions_lookup(
        df=df,
        col_name_exec_id="EXEC_ID",
    )

    cols = [
        "EXEC_ID",
        "REGION",
        "NATIONAL_BANNER_DESC",
        "SECTION_MASTER",
    ]

    assert df_res.select(*cols).exceptAll(df.select(*cols)).limit(1).count() == 0


def test_pivot_result(spark: SparkSession):

    df = spark.createDataFrame(
        [
            Row(
                EXEC_ID="EXEC_ID_1",
                ITEM_NO="1",
                index=1,
                EMBEDD_NAME="EMBEDD_NAME_1",
                EMBEDD_VALUE=10,
            ),
            Row(
                EXEC_ID="EXEC_ID_1",
                ITEM_NO="1",
                index=1,
                EMBEDD_NAME="EMBEDD_NAME_2",
                EMBEDD_VALUE=20,
            ),
        ]
    )

    df_expected = spark.createDataFrame(
        [
            Row(
                EXEC_ID="EXEC_ID_1",
                ITEM_NO="1",
                index="1",
                EMBEDD_NAME_1="10",
                EMBEDD_NAME_2="20",
            )
        ]
    )

    df_res = pivot_result(df)

    assert df_res.exceptAll(df_expected).limit(1).count() == 0


def test_lookup_region_banner_pog(spark: SparkSession):

    df = spark.createDataFrame(
        [
            Row(
                EXEC_ID="EXEC_ID_1",
            ),
            Row(
                EXEC_ID="EXEC_ID_2",
            ),
        ]
    )

    df_cust_item_diff = spark.createDataFrame(
        [
            Row(
                EXEC_ID="EXEC_ID_1",
                REGION="REGION 1",
                NATIONAL_BANNER_DESC="NATIONAL_BANNER_DESC 1",
                SECTION_MASTER="SECTION_MASTER 1",
            ),
            Row(
                EXEC_ID="EXEC_ID_2",
                REGION="REGION 2",
                NATIONAL_BANNER_DESC="NATIONAL_BANNER_DESC 2",
                SECTION_MASTER="SECTION_MASTER 2",
            ),
        ]
    )

    df_res = lookup_region_banner_pog(
        df=df,
        df_cust_item_diff=df_cust_item_diff,
        col_name_exec_id="EXEC_ID",
    )

    assert df_res.exceptAll(df_cust_item_diff).limit(1).count() == 0

    # try invalid

    df_cust_item_diff = spark.createDataFrame(
        [
            Row(
                EXEC_ID="EXEC_ID_1",
                REGION="REGION 1",
                NATIONAL_BANNER_DESC="NATIONAL_BANNER_DESC 1",
                SECTION_MASTER="SECTION_MASTER 1",
            ),
            Row(
                EXEC_ID="EXEC_ID_1",
                REGION="REGION 2",
                NATIONAL_BANNER_DESC="NATIONAL_BANNER_DESC 2",
                SECTION_MASTER="SECTION_MASTER 2",
            ),
        ]
    )

    broke = False

    try:

        lookup_region_banner_pog(
            df=df,
            df_cust_item_diff=df_cust_item_diff,
            col_name_exec_id="EXEC_ID",
        )
    except AssertionError:
        tb = traceback.format_exc()
        log.info(f"Broke here:\n{tb}\n")
        broke = True

    assert broke, "must break here"


def test_log_entity_counts_ns(spark: SparkSession):

    df_prod2vec_selected = spark.createDataFrame(
        [
            Row(
                EXEC_ID="EXEC_ID_1",
                ITEM_A="1",
                ITEM_B="2",
            ),
            Row(
                EXEC_ID="EXEC_ID_2",
                ITEM_A="3",
                ITEM_B="4",
            ),
        ]
    )

    df_exec_id_run_dimensions_lookup = spark.createDataFrame(
        [
            Row(
                EXEC_ID="EXEC_ID_1",
                REGION="REGION 1",
                NATIONAL_BANNER_DESC="NATIONAL_BANNER_DESC 1",
                SECTION_MASTER="SECT MASTER 1",
            ),
            Row(
                EXEC_ID="EXEC_ID_2",
                REGION="REGION 2",
                NATIONAL_BANNER_DESC="NATIONAL_BANNER_DESC 2",
                SECTION_MASTER="SECT MASTER 2",
            ),
        ]
    )

    # TODO: assert that correct result is being logged
    #  (need to capture stdout from logging)

    log_entity_counts_staged_pairs(
        df_prod2vec_selected=df_prod2vec_selected,
        df_exec_id_run_dimensions_lookup=df_exec_id_run_dimensions_lookup,
    )

    df = df_prod2vec_selected.withColumnRenamed("ITEM_A", "ITEM_NO").join(
        df_exec_id_run_dimensions_lookup, "EXEC_ID", "inner"
    )

    log_entity_counts_prod2vec(df)
