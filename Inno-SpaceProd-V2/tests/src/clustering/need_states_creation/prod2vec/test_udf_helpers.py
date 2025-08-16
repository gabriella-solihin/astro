import numpy as np
import pandas as pd
from typing import Dict
import pytest
from pyspark.sql import SparkSession

from spaceprod.src.clustering.need_states_creation.prod2vec.udf_helpers import (
    parse_embeddings,
    prepare_embeddings_for_output,
    udf_out_processing,
    udf_out_add_static_values,
    udf_out_type_casting,
    prepare_exception_messages_for_output,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()
    return spark


@pytest.fixture(scope="session")
def item_embeddings_from_model():
    item_embeddings_from_model = [[1, 2, 3], [4, 5, 6]]
    return item_embeddings_from_model


@pytest.fixture(scope="session")
def reversed_dictionary():
    reversed_dictionary = {0: "000", 1: "111"}
    return reversed_dictionary


def test_parse_embeddings(
    item_embeddings_from_model: np.ndarray, reversed_dictionary: Dict[int, str]
) -> pd.DataFrame:

    pdf_output = parse_embeddings(item_embeddings_from_model, reversed_dictionary)

    expected_cols = ["embedd_0", "embedd_1", "embedd_2", "index", "ITEM_NO"]
    expected_index = [0, 1]
    expected_item_no = ["000", "111"]

    # check columns are renamed as expected
    assert (
        list(pdf_output.columns) == expected_cols
    ), f"should receive {expected_cols} but got {list(pdf_output.columns)} instead"

    # check column "index" is created properly
    assert (
        list(pdf_output["index"]) == expected_index
    ), f"should receive {expected_index} but got {list(pdf_output.index)} instead"

    # check mapping using reversed dictionary is done properly
    assert (
        list(pdf_output["ITEM_NO"]) == expected_item_no
    ), f'should receive {expected_item_no} but got {list(pdf_output["ITEM_NO"])} instead'


@pytest.fixture(scope="session")
def pdf_embeddings():
    pdf_embeddings = pd.DataFrame(data=["UNK", 1], columns=["ITEM_NO"])
    return pdf_embeddings


def test_prepare_embeddings_for_output(
    pdf_embeddings: pd.DataFrame,
) -> pd.DataFrame:

    output = prepare_embeddings_for_output(pdf_embeddings)

    assert "UNK" not in list(output["ITEM_NO"]), f"UNK should be replaced by -1"

    assert (
        output["ITEM_NO"].apply(float.is_integer).all()
    ), f"ITEM_NO column should be all integers."


@pytest.fixture(scope="session")
def pdf_model_output():

    pdf_model_output = pd.DataFrame(
        data=[[0.1, 0.2, 0.3, 0, 1]],
        columns=["embedd_0", "embedd_1", "embedd_2", "index", "ITEM_NO"],
    )

    return pdf_model_output


def test_udf_out_processing(pdf_model_output: pd.DataFrame) -> pd.DataFrame:

    pdf_result_melt = udf_out_processing(pdf_model_output)

    # ensure columns in melt output is right
    expected_columns = ["ITEM_NO", "index", "EMBEDD_NAME", "EMBEDD_VALUE"]
    output_columns = list(pdf_result_melt.columns)
    assert (
        output_columns == expected_columns
    ), f"expect columns {expected_columns} but got {output_columns}"

    mask_value0 = (
        pdf_result_melt[pdf_result_melt["EMBEDD_NAME"] == "embedd_0"]["EMBEDD_VALUE"][0]
        == 0.1
    )

    mask_value1 = (
        pdf_result_melt[pdf_result_melt["EMBEDD_NAME"] == "embedd_1"]["EMBEDD_VALUE"][1]
        == 0.2
    )

    mask_value2 = (
        pdf_result_melt[pdf_result_melt["EMBEDD_NAME"] == "embedd_2"]["EMBEDD_VALUE"][2]
        == 0.3
    )

    assert mask_value0, f"expect 0.1 but got other values"
    assert mask_value1, f"expect 0.2 but got other values"
    assert mask_value2, f"expect 0.3 but got other values"


@pytest.fixture(scope="session")
def exec_id():
    return "ontario_SOBEYS_FrozenDessert"


@pytest.fixture(scope="session")
def pdf():
    return pd.DataFrame(data=[[1, 2], [3, 4]], columns=["col_a", "col_b"])


def test_udf_out_add_static_values(
    pdf: pd.DataFrame,
    exec_id: str,
) -> pd.DataFrame:

    out_df = udf_out_add_static_values(pdf, exec_id)

    assert (
        out_df[["EXEC_ID"]].drop_duplicates()["EXEC_ID"][0] == exec_id
    ), f"new column EXEC_ID is not added properly."


@pytest.fixture(scope="session")
def expected_schema():
    expected_schema = {
        "ITEM_NO": float,
        "index": int,
        "EMBEDD_NAME": str,
        "EMBEDD_VALUE": float,
        "EXEC_ID": str,
    }
    return expected_schema


@pytest.fixture(scope="session")
def in_pdf():

    data = [[1, 0, "embedd_1", "0.55", "run1"]]
    col = ["ITEM_NO", "index", "EMBEDD_NAME", "EMBEDD_VALUE", "EXEC_ID"]
    in_pdf = pd.DataFrame(data=data, columns=col)
    return in_pdf


def test_udf_out_type_casting(
    in_pdf: pd.DataFrame, expected_schema: Dict
) -> pd.DataFrame:

    out_df = udf_out_type_casting(in_pdf)
    out_schema = out_df.dtypes.apply(lambda x: x.name).to_dict()

    for col in ["ITEM_NO", "EMBEDD_VALUE"]:
        assert (
            "float" in out_schema[col]
        ), f"expect float type but got {out_schema[col]}"

    assert "int" in out_schema["index"], f"expect int type but got {out_schema[col]}"

    for col in ["EMBEDD_NAME", "EXEC_ID"]:
        assert (
            "object" in out_schema[col]
        ), f"expect object(str) type but got {out_schema[col]}"


@pytest.fixture(scope="session")
def msg_exception():
    return "msg_exception"


@pytest.fixture(scope="session")
def msg_traceback():
    return "msg_traceback"


def test_prepare_exception_messages_for_output(
    exec_id: str,
    msg_exception: str,
    msg_traceback: str,
) -> pd.DataFrame:

    out_df = prepare_exception_messages_for_output(
        exec_id, msg_exception, msg_traceback
    )
    assert (
        msg_traceback in out_df["EMBEDD_NAME"][0]
    ), f"{msg_traceback} not in EMBEDD_NAME"

    assert (
        msg_exception in out_df["EMBEDD_NAME"][0]
    ), f"{msg_exception} not in EMBEDD_NAME"
