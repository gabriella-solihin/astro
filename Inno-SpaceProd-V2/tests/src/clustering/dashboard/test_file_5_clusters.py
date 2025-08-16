import pandas as pd
import pytest

from pyspark.sql import SparkSession
from spaceprod.src.clustering.dashboard.file_5_clusters.helpers import (
    run_external_cluster_competition_dashboard,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()
    return spark


@pytest.fixture(scope="session")
def df_index():
    df_index = pd.DataFrame(
        {
            "Cluster_Labels": {
                0: 1,
                1: 1,
                2: 1,
            },
            "var": {
                0: "competitor_banner_density",
                1: "competitor_banner_density",
                2: "competitor_banner_density",
            },
            "var_details": {
                0: "competitor_banner_density_details_9999",
                1: "competitor_banner_density_details_9999",
                2: "competitor_banner_density_details_9999",
            },
            "Index": {
                0: 98,
                1: 96,
                2: 94,
            },
            "Index_Stdev": {
                0: 2,
                1: 2,
                2: 2,
            },
            "REGION": {
                0: "Ontario",
                1: "Ontario",
                2: "Ontario",
            },
            "BANNER": {
                0: "FRESH CO",
                1: "FRESH CO",
                2: "FRESH CO",
            },
        }
    )
    return df_index


@pytest.fixture(scope="session")
def df_name():
    df_name = pd.DataFrame(
        [
            {
                "var_details": "competitor_banner_density_details_9999",
                "Cmp Feature": "details_9999",
                "Flag": 1,
            },
        ]
    )
    return df_name


def test_run_external_cluster_competition_dashboard(
    df_index: pd.DataFrame, df_name: pd.DataFrame
) -> pd.DataFrame:

    pdf_output = run_external_cluster_competition_dashboard(
        pdf=df_index,
        pdf_dashboard_names=df_name,
    )

    val_act = int(pdf_output[pdf_output["Index"] == 98]["Rank"].values[0])
    val_exp = 3
    assert val_act == val_exp, f"Rank supposed to be {val_exp}"

    val_act = int(pdf_output[pdf_output["Index"] == 96]["Rank"].values[0])
    val_exp = 2
    assert val_act == val_exp, f"Rank supposed to be {val_exp}"

    val_act = int(pdf_output[pdf_output["Index"] == 94]["Rank"].values[0])
    val_exp = 1
    assert val_act == val_exp, f"Rank supposed to be {val_exp}"

    len_input = len(df_index)
    len_output = len(pdf_output)
    assert len_input == len_output, f"len() of input does not match that of output."

    # change the data, the result should filter out invalid data
    df_index.loc[0, "Index_Stdev"] = None
    pdf_output = run_external_cluster_competition_dashboard(df_index, df_name)
    len_input = len(df_index)
    len_output = len(pdf_output)
    assert len_input - len_output == 1, "output should be 1 row less than input"
