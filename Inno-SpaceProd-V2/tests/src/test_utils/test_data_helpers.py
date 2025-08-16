import pandas as pd
from pandas.testing import assert_frame_equal

from unittest import mock

from spaceprod.src.clustering.external_clustering.feature_clustering.udf import (
    normalized,
)
from spaceprod.utils.data_helpers import (
    strip_except_alpha_num,
    snake_case,
    filter_and_assert_not_empty,
    write_pickles,
    read_pickles,
)
import pytest


def test_strip_except_alpha_num():
    """This function remove all non-word characters (everything except numbers and letters) in a column

    Returns
    -------
    modified_s_2: str
        modified column
    """
    # test the outputs of the function is snake case with fist letter in upper case
    s_1 = "aBc_deF"
    modified_s_1 = strip_except_alpha_num(s_1)
    assert modified_s_1 == "Abc_Def"

    s_2 = "abc def"
    modified_s_2 = strip_except_alpha_num(s_2)
    assert modified_s_2 == "Abc_Def"


def test_snake_case():
    """This function tests the outputs of the function is snake case and
    it remove all non-word characters (everything except numbers and letters)

    Returns
    -------
    modified_s: str
        modified column
    """

    s = "aBc_de121F# !rea"
    modified_s = snake_case(s)
    assert modified_s == "aBc_de121F_rea"


def test_normalized():
    """This function tests the normalization and asserts an expected normalized df"""

    df = pd.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
    normalized_df = normalized(df)
    expected_normalized_df = pd.DataFrame({"a": [0.0, 0.5, 1.0], "b": [0.0, 0.5, 1.0]})
    assert_frame_equal(normalized_df, expected_normalized_df)


def test_filter_and_assert_not_empty():
    """This function filters and asserts a dataframe is not empty"""
    df = pd.DataFrame({"A": [2, 2, 1], "B": ["a", "b", "c"]})
    df_name = "test_df"
    filter_col = "A"
    filter_value = 2
    filtered_df = filter_and_assert_not_empty(df, df_name, filter_col, filter_value)
    expected_filtered_df = pd.DataFrame({"A": [2, 2], "B": ["a", "b"]})
    assert_frame_equal(filtered_df, expected_filtered_df)


@mock.patch("spaceprod.utils.data_helpers.pickle.dump")
@mock.patch("spaceprod.utils.data_helpers.upload_to_blob")
@pytest.mark.skip  # TODO: address: https://dev.azure.com/SobeysInc/Inno-SpaceProd/_TestManagement/Runs?runId=108442
def test_write_pickles(mock_upload_to_blob, mock_pickle_dump):
    """This function tests writing pickles to the blob

    input can be any object
    """
    path = "to/path"
    file = "file"

    with mock.patch("builtins.open", mock.mock_open(read_data="file")) as mock_file:
        write_pickles(path, file, prefix="abc", local=True)
        assert mock_pickle_dump.call_count == 1
        assert mock_upload_to_blob.call_count == 0

    write_pickles(path, file, prefix="abc", local=False)
    assert mock_upload_to_blob.call_count == 0 + 1
    assert mock_upload_to_blob.call_args[0] == ("to/path", "/tmp/abc.pkl")

    assert mock_pickle_dump.call_count == 1 + 1
    assert mock_pickle_dump.call_args[0][0] == "file"
    assert mock_pickle_dump.call_args[0][1].name == "/tmp/abc.pkl"


@mock.patch("spaceprod.utils.data_helpers.pickle.load")
@mock.patch("spaceprod.utils.data_helpers.download_from_blob")
@pytest.mark.skip  # TODO: address: https://dev.azure.com/SobeysInc/Inno-SpaceProd/_TestManagement/Runs?runId=108442
def test_read_pickles(mock_download_from_blob, mock_pickle_load):
    """This function tests reading pickles to the blob

    input can be any object
    """
    path = "to/path"

    with mock.patch("builtins.open", mock.mock_open(read_data="file")) as mock_file:
        read_pickles(path, prefix="abc", local=True)
        assert mock_pickle_load.call_count == 1
        assert mock_download_from_blob.call_count == 0

    read_pickles(path, prefix="abc", local=False)
    assert mock_pickle_load.call_count == 1 + 1
    assert mock_download_from_blob.call_count == 0 + 1
    assert mock_download_from_blob.call_args[0] == ("to/path", "/tmp/abc.pkl")
    assert mock_pickle_load.call_args[0][0].name == "/tmp/abc.pkl"
