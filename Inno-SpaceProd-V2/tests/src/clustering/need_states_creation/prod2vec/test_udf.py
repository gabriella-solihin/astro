import pytest

from pyspark import Row
from pyspark.sql import SparkSession
from pyspark.sql.udf import UserDefinedFunction
from spaceprod.src.clustering.need_states_creation.prod2vec.udf import (
    determine_timeout_for_modelling,
    generate_prod2vec_udf,
)
from spaceprod.utils.data_helpers import backup_on_blob

from spaceprod.utils.data_transformation import pyspark_df_col_to_unique_list


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()
    return spark


@pytest.mark.parametrize(
    "num_epoches_input, expected_output",
    [
        (8, 10),  # case 1 for max(num_epochs * per_epoc_min, 10)
        (10, 12),  # case 2 for max(num_epochs * per_epoc_min, 10)
        (99, 119),  # case 1 for min(120, timeout_min)
        (101, 120),  # case 2 for min(120, timeout_min)
    ],
)
def test_determine_timeout_for_modelling(num_epoches_input, expected_output):

    output = determine_timeout_for_modelling(num_epoches_input)

    assert (
        output == expected_output
    ), f"Should receive {expected_output} but got {output}"


def test_prod2vec_udf(spark: SparkSession):
    df = spark.createDataFrame(
        [
            Row(
                CUSTOMER_CARD_ID="8006757629",
                ITEM_A="375039",
                ITEM_B="955963",
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
            ),
            Row(
                CUSTOMER_CARD_ID="8009789087",
                ITEM_A="678984",
                ITEM_B="375030",
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
            ),
            Row(
                CUSTOMER_CARD_ID="8412691483",
                ITEM_A="819649",
                ITEM_B="375030",
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
            ),
            Row(
                CUSTOMER_CARD_ID="8901137514",
                ITEM_A="955963",
                ITEM_B="375039",
                EXEC_ID="ontario_SOBEYS_FrozenPizza",
            ),
        ]
    )

    path_prod2vec_staged_data = backup_on_blob(
        spark=spark,
        df=df,
        get_path=True,
        partition_by=["EXEC_ID"],
    )

    udf: UserDefinedFunction = generate_prod2vec_udf(
        batch_size=32,
        num_ns=5,
        max_num_pairs=50,
        embedding_size=128,
        item_embeddings_layer_name="p2v_embedding",
        num_epochs=5,
        steps_per_epoch=100,
        early_stopping_patience=100,
        path_prod2vec_staged_data=path_prod2vec_staged_data,
    )

    # call the udf
    pdf = df.toPandas()
    pdf_res = udf.func(pdf)

    cols = ["ITEM_NO", "index", "EMBEDD_NAME", "EMBEDD_VALUE", "EXEC_ID"]
    assert set(cols) == set(list(pdf_res.columns))
    assert len(pdf_res) == 768

    list_exp_a = pyspark_df_col_to_unique_list(df, "ITEM_A")
    list_exp_b = pyspark_df_col_to_unique_list(df, "ITEM_B")
    list_exp = list(set(list_exp_a + list_exp_b + ["-1"]))
    list_act = pdf_res["ITEM_NO"].astype(int).astype(str).tolist()
    assert set(list_act) == set(list_exp)
