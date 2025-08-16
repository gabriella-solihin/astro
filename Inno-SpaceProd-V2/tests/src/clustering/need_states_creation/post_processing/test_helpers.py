import pandas as pd
import pytest

from pyspark import Row
from pyspark.sql import SparkSession
from spaceprod.src.clustering.need_states_creation.post_processing.udf import (
    generate_need_states_udf,
)

from spaceprod.src.clustering.need_states_creation.post_processing.helpers import (
    add_prod_descs_to_cosine_sim,
    calc_cosine_sim,
    create_vectors_from_cols,
    generate_need_states_all_pogs,
    process_items_lost_ns_explosion,
    process_resulting_dist_matrix_data,
    process_resulting_need_state_data,
    replace_negative_cosine_similarity,
    reshape_data_for_udf,
    create_dim_lookup,
)
from spaceprod.utils.imports import F
from spaceprod.utils.validation import dup_check


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    from tests.utils.spark_util import spark_tests as spark

    return spark


def test_create_vectors_from_cols(spark):

    df = spark.createDataFrame(
        [
            Row(
                EXEC_ID="exec_id_1",
                ITEM_NO="1",
                index="0",
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Frozen Handhelds",
                embedd_0=0.05,
                embedd_1=-0.03,
            ),
            Row(
                EXEC_ID="exec_id_1",
                ITEM_NO="2",
                index="0",
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Frozen Handhelds",
                embedd_0=0.05,
                embedd_1=-0.03,
            ),
            Row(
                EXEC_ID="exec_id_2",
                ITEM_NO="3",
                index="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Frozen Pizza",
                embedd_0=0.02,
                embedd_1=0.04,
            ),
            Row(
                EXEC_ID="exec_id_2",
                ITEM_NO="4",
                index="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Frozen Pizza",
                embedd_0=0.02,
                embedd_1=0.04,
            ),
        ]
    )

    df_result = create_vectors_from_cols(df=df)

    # all vectors should be of length 2
    assert len(df_result.toPandas()["vector"].apply(len).loc[lambda x: x != 2]) == 0

    # add another embedding, all vectors should be of length 3
    df = df.withColumn("embedd_2", F.lit(0))
    df_result = create_vectors_from_cols(df=df)
    assert len(df_result.toPandas()["vector"].apply(len).loc[lambda x: x != 3]) == 0


def test_calc_cosine_sim(spark):

    df = spark.createDataFrame(
        [
            Row(
                EXEC_ID="exec_id_1",
                ITEM_NO="1",
                index="0",
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Frozen Handhelds",
                vector=[1, 1],
            ),
            Row(
                EXEC_ID="exec_id_1",
                ITEM_NO="2",
                index="0",
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Frozen Handhelds",
                vector=[2, 2],
            ),
            Row(
                EXEC_ID="exec_id_2",
                ITEM_NO="3",
                index="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Frozen Pizza",
                vector=[-10, -20],
            ),
            Row(
                EXEC_ID="exec_id_2",
                ITEM_NO="4",
                index="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="SOBEYS",
                SECTION_MASTER="Frozen Pizza",
                vector=[10, 20],
            ),
        ]
    )

    df_result = calc_cosine_sim(df=df)
    pdf_result = df_result.toPandas()

    assert all(pdf_result.query("EXEC_ID_A=='exec_id_1'")["COSINE_SIMILARITY"] == 1.0)
    assert all(pdf_result.query("EXEC_ID_A=='exec_id_2'")["COSINE_SIMILARITY"] == -1.0)


def test_add_prod_descs_to_cosine_sim(spark):

    df = spark.createDataFrame(
        [
            Row(
                EXEC_ID_A="exec_id_1",
                ITEM_A="1",
                EXEC_ID_B="exec_id_1",
                ITEM_B="2",
                COSINE_SIMILARITY="1.0",
            ),
            Row(
                EXEC_ID_A="exec_id_1",
                ITEM_A="2",
                EXEC_ID_B="exec_id_1",
                ITEM_B="1",
                COSINE_SIMILARITY="1.0",
            ),
            Row(
                EXEC_ID_A="exec_id_2",
                ITEM_A="3",
                EXEC_ID_B="exec_id_2",
                ITEM_B="4",
                COSINE_SIMILARITY="-1.0",
            ),
            Row(
                EXEC_ID_A="exec_id_2",
                ITEM_A="4",
                EXEC_ID_B="exec_id_2",
                ITEM_B="3",
                COSINE_SIMILARITY="-1.0",
            ),
        ]
    )

    df_prod = spark.createDataFrame(
        [
            Row(ITEM_NO="1", ITEM_NAME="A"),
            Row(ITEM_NO="2", ITEM_NAME="B"),
            Row(ITEM_NO="3", ITEM_NAME="C"),
            Row(ITEM_NO="4", ITEM_NAME="C"),
        ]
    )

    df_result = add_prod_descs_to_cosine_sim(df=df, df_prod=df_prod).cache()

    assert df_result.filter(F.isnull(F.col("ITEM_ENG_DESC_A"))).limit(1).count() == 0
    assert df_result.filter(F.isnull(F.col("ITEM_ENG_DESC_B"))).limit(1).count() == 0

    # if product data did not have any matches, we should get all nulls

    df_prod = spark.createDataFrame([Row(ITEM_NO="5", ITEM_NAME="A")])
    df_result = add_prod_descs_to_cosine_sim(df=df, df_prod=df_prod).cache()
    assert df_result.filter(F.isnull(F.col("ITEM_ENG_DESC_A"))).count() == 4
    assert df_result.filter(F.isnull(F.col("ITEM_ENG_DESC_B"))).count() == 4


def test_replace_negative_cosine_similarity(spark):
    df = spark.createDataFrame([Row(COSINE_SIMILARITY=-1), Row(COSINE_SIMILARITY=1)])

    df_expected = spark.createDataFrame(
        [
            Row(COSINE_SIMILARITY="0", COSINE_SIMILARITY_MIN="1"),
            Row(COSINE_SIMILARITY="1", COSINE_SIMILARITY_MIN="0"),
        ]
    )

    df_result = replace_negative_cosine_similarity(df)

    assert df_result.exceptAll(df_expected).limit(1).count() == 0

    # try with no negative values, nothing should be replaced
    df = spark.createDataFrame([Row(COSINE_SIMILARITY=1), Row(COSINE_SIMILARITY=1)])
    df_result = replace_negative_cosine_similarity(df)
    df_expected = df.withColumn("COSINE_SIMILARITY_MIN", F.lit(0))
    assert df_result.exceptAll(df_expected).limit(1).count() == 0


def test_reshape_data_for_udf(spark):

    df = spark.createDataFrame(
        [
            Row(
                ITEM_A="1",
                ITEM_B="10",
                ITEM_ENG_DESC_A="desc_1",
                ITEM_ENG_DESC_B="desc_10",
                COSINE_SIMILARITY=0.6,
                COSINE_SIMILARITY_MIN=0.1,
                EXEC_ID="exec_id_a",
            ),
            Row(
                ITEM_A="2",
                ITEM_B="20",
                ITEM_ENG_DESC_A="desc_2",
                ITEM_ENG_DESC_B="desc_20",
                COSINE_SIMILARITY=0.7,
                COSINE_SIMILARITY_MIN=0.2,
                EXEC_ID="exec_id_b",
            ),
            Row(
                ITEM_A="3",
                ITEM_B="30",
                ITEM_ENG_DESC_A="desc_3",
                ITEM_ENG_DESC_B="desc_30",
                COSINE_SIMILARITY=0.8,
                COSINE_SIMILARITY_MIN=0.3,
                EXEC_ID="exec_id_c",
            ),
        ]
    )

    df_prod = spark.createDataFrame(
        [
            Row(ITEM_NO="1", ITEM_NAME="name_1"),
            Row(ITEM_NO="2", ITEM_NAME="name_2"),
            Row(ITEM_NO="3", ITEM_NAME="name_3"),
        ]
    )

    df_res = reshape_data_for_udf(
        df=df,
        df_prod=df_prod,
    ).cache()

    # expected values for 'exec_id_a'
    val_item_cosine_data = [
        {
            "ITEM_ENG_DESC_A": "desc_1",
            "ITEM_ENG_DESC_B": "desc_10",
            "ITEM_B": "10",
            "COSINE_SIMILARITY": "0.6",
            "COSINE_SIMILARITY_MIN": "0.1",
            "ITEM_A": "1",
        }
    ]

    val_item_desc_data = [
        {"ITEM_NO": "1", "ITEM_NAME": "name_1"},
        {"ITEM_NO": "2", "ITEM_NAME": "name_2"},
        {"ITEM_NO": "3", "ITEM_NAME": "name_3"},
    ]

    cols_exp = ["EXEC_ID", "ITEM_COSINE_DATA", "ITEM_DESC_DATA"]
    assert set(df_res.columns) == set(cols_exp), "wrong cols"

    mask = F.col("EXEC_ID") == "exec_id_a"
    assert df_res.filter(mask).collect()[0]["ITEM_COSINE_DATA"] == val_item_cosine_data

    mask = F.col("EXEC_ID") == "exec_id_b"
    assert df_res.filter(mask).collect()[0]["ITEM_DESC_DATA"] == val_item_desc_data


def test_generate_need_states_all_pogs(spark):

    list_cosine_sim = [
        {
            "ITEM_B": "1",
            "COSINE_SIMILARITY": "0.1",
            "COSINE_SIMILARITY_MIN": "0.2",
            "ITEM_A": "2",
        },
        {
            "ITEM_B": "2",
            "COSINE_SIMILARITY": "0.1",
            "COSINE_SIMILARITY_MIN": "0.2",
            "ITEM_A": "1",
        },
    ]

    list_item_desc = [
        {"ITEM_NO": "1", "ITEM_NAME": "item_1"},
        {"ITEM_NO": "2", "ITEM_NAME": "item_2"},
    ]

    result_expected = (
        [
            {"NEED_STATE": "0", "ITEM_NO": "1", "ITEM_NAME": "item_1"},
            {"NEED_STATE": "0", "ITEM_NO": "2", "ITEM_NAME": "item_2"},
        ],
        [
            {"ITEM_A": "1", "ITEM_B": "1", "value": "0.0"},
            {"ITEM_A": "2", "ITEM_B": "1", "value": "0.2"},
            {"ITEM_A": "1", "ITEM_B": "2", "value": "0.2"},
            {"ITEM_A": "2", "ITEM_B": "2", "value": "0.0"},
        ],
    )

    result = generate_need_states_udf(
        list_cosine_sim=list_cosine_sim,
        list_item_desc=list_item_desc,
        min_cluster_size=1,
        deep_split=1,
    )

    assert result == result_expected


def test_process_resulting_need_state_data(spark):
    pdf_mock = pd.DataFrame(
        {
            "EXEC_ID": {
                0: "exec_0",
                1: "exec_1",
            },
            "MODEL_OUTPUT_DATA": {
                0: Row(
                    need_states=[
                        {
                            "NEED_STATE": "2",
                            "ITEM_NO": "1",
                            "ITEM_NAME": "DrOetker Ristornt Pollo Pizza",
                        },
                        {
                            "NEED_STATE": "1",
                            "ITEM_NO": "2",
                            "ITEM_NAME": "DrOetKer Giusep Thin Crst Spin",
                        },
                        {
                            "NEED_STATE": "1",
                            "ITEM_NO": "3",
                            "ITEM_NAME": "DrOetker Glusep Thin Can  510g",
                        },
                    ],
                    dist_matrix=[0],
                ),
                1: Row(
                    need_states=[
                        {
                            "NEED_STATE": "1",
                            "ITEM_NO": "4",
                            "ITEM_NAME": "Delisso Pzria Deluxe",
                        },
                        {
                            "NEED_STATE": "1",
                            "ITEM_NO": "5",
                            "ITEM_NAME": "Delissio RsnC Peprn 12IN",
                        },
                        {
                            "NEED_STATE": "2",
                            "ITEM_NO": "6",
                            "ITEM_NAME": "DrOetker Pza CasaDMa ClsscCdn",
                        },
                    ],
                    dist_matrix=[0],
                ),
            },
        }
    )

    df_mock = spark.createDataFrame(pdf_mock)

    df_special_ns_mock = spark.createDataFrame(
        [
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                ITEM_NO="7",
                NEED_STATE="-1",
                ITEM_NAME="SMI Opsite 24632 Dr 5x5in  1ea",
                ITEM_LOST_IN_NS_REASON="REASON_1",
            ),
            Row(
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
                ITEM_NO="8",
                NEED_STATE="-1",
                ITEM_NAME="Delissio RsnC Spcy Chicken",
                ITEM_LOST_IN_NS_REASON="REASON_2",
            ),
        ]
    )

    df_prod_mock = spark.createDataFrame(
        [
            Row(ITEM_SK="1", ITEM_NO="1"),
            Row(ITEM_SK="2", ITEM_NO="2"),
            Row(ITEM_SK="3", ITEM_NO="3"),
            Row(ITEM_SK="4", ITEM_NO="4"),
            Row(ITEM_SK="5", ITEM_NO="5"),
            Row(ITEM_SK="6", ITEM_NO="6"),
            Row(ITEM_SK="7", ITEM_NO="7"),
            Row(ITEM_SK="8", ITEM_NO="8"),
        ]
    )

    cols_dummy = [
        "LVL5_NAME",
        "LVL5_ID",
        "LVL2_ID",
        "LVL3_ID",
        "LVL4_NAME",
        "LVL3_NAME",
        "LVL2_NAME",
        "CATEGORY_ID",
    ]

    cols = [F.lit("dummy").alias(x) for x in cols_dummy] + ["ITEM_SK", "ITEM_NO"]
    df_prod_mock = df_prod_mock.select(*cols)

    df_res = process_resulting_need_state_data(
        df=df_mock,
        df_special_ns=df_special_ns_mock,
        df_prod=df_prod_mock,
    )

    df_res.cache()

    # ensuring all expected columns are present
    cols_new = [
        "ITEM_NAME",
        "ITEM_SK",
        "need_state",
        "EXEC_ID",
        "cannib_id",
        "ITEM_NO",
        "ITEM_LOST_IN_NS_REASON",
    ]

    cols_expected = cols_dummy + cols_new
    assert set(df_res.columns) == set(cols_expected)

    # must have all 8 products from the input data
    assert df_res.count() == 8

    # must have all 2 need states + invalid (-1) need state for total of 3
    assert df_res.select("need_state").dropDuplicates().count() == 3


def test_process_resulting_dist_matrix_data(spark):

    pdf_mock = pd.DataFrame(
        {
            "EXEC_ID": {
                0: "exec_id_0",
                1: "exec_id_1",
            },
            "MODEL_OUTPUT_DATA": {
                0: Row(
                    need_states=[1],
                    dist_matrix=[
                        {"ITEM_B": "10", "value": "0.0", "ITEM_A": "30"},
                        {"ITEM_B": "20", "value": "1.0", "ITEM_A": "40"},
                    ],
                ),
                1: Row(
                    need_states=[1],
                    dist_matrix=[
                        {"ITEM_B": "1", "value": "0.0", "ITEM_A": "3"},
                        {"ITEM_B": "2", "value": "1.0", "ITEM_A": "4"},
                    ],
                ),
            },
        }
    )

    df_mock = spark.createDataFrame(pdf_mock)

    df_expected = spark.createDataFrame(
        [
            Row(EXEC_ID="exec_id_0", ITEM_A="30", ITEM_B="10", VALUE="0.0"),
            Row(EXEC_ID="exec_id_0", ITEM_A="40", ITEM_B="20", VALUE="1.0"),
            Row(EXEC_ID="exec_id_1", ITEM_A="3", ITEM_B="1", VALUE="0.0"),
            Row(EXEC_ID="exec_id_1", ITEM_A="4", ITEM_B="2", VALUE="1.0"),
        ]
    )

    df_res = process_resulting_dist_matrix_data(df_mock)

    assert df_res.exceptAll(df_expected).limit(1).count() == 0


def test_create_dim_lookup(spark):

    df_item_embed = spark.createDataFrame(
        [
            Row(EXEC_ID="e1", REGION="a", NATIONAL_BANNER_DESC="s", SECTION_MASTER="1"),
            Row(EXEC_ID="e2", REGION="b", NATIONAL_BANNER_DESC="s", SECTION_MASTER="2"),
            Row(EXEC_ID="e3", REGION="c", NATIONAL_BANNER_DESC="s", SECTION_MASTER="3"),
        ]
    )

    df_special_ns = spark.createDataFrame(
        [
            Row(EXEC_ID="e4", REGION="a", NATIONAL_BANNER_DESC="s", SECTION_MASTER="4"),
            Row(EXEC_ID="e5", REGION="b", NATIONAL_BANNER_DESC="s", SECTION_MASTER="5"),
            Row(EXEC_ID="e6", REGION="c", NATIONAL_BANNER_DESC="s", SECTION_MASTER="6"),
        ]
    )

    df_res = create_dim_lookup(
        df_item_embed=df_item_embed,
        df_special_ns=df_special_ns,
    )

    df_res.cache()

    assert df_res.count() == 6
    dup_check(df_res, ["EXEC_ID"])
    assert set(df_res.columns) == set(df_item_embed.columns)
