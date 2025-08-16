import pandas as pd
import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from spaceprod.src.optimization.post_process.helpers import (
    get_projected_sales_margin,
)
from spaceprod.utils.imports import F


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    spark = SparkSession.builder.getOrCreate()
    return spark


def test_get_projected_sales_margin(spark):
    df_opt = spark.createDataFrame(
        [
            Row(
                Store_Physical_Location_No=1,
                Region_Desc="r1",
                BANNER="b1",
                SECTION_MASTER="s1",
                ITEM_NO="i1",
                Department="d1",
                Optim_Facings=1,
                Current_Facings=1,
                Opt_Width_X_Facing=1,
                Cur_Width_X_Facing=1,
            ),
        ]
    )

    df_clusters = spark.createDataFrame(
        [
            Row(
                Store_Physical_Location_No=1,
                M_cluster="m1",
            ),
        ]
    )

    elas_sales = spark.createDataFrame(
        [
            Row(
                National_Banner_Desc="b1",
                Region="r1",
                SECTION_MASTER="s1",
                ITEM_NO="i1",
                M_cluster="m1",
            ),
        ]
    )

    elas_margins = spark.createDataFrame(
        [
            Row(
                National_Banner_Desc="b1",
                Region="r1",
                SECTION_MASTER="s1",
                ITEM_NO="i1",
                M_cluster="m1",
            ),
        ]
    )

    cols_facings = ["facing_fit_" + str(i) for i in range(13)]

    # add dummy values for facings
    for col in cols_facings:
        elas_sales = elas_sales.withColumn(col, F.lit(0))
        elas_margins = elas_margins.withColumn(col, F.lit(0))

    pdf_expected = pd.DataFrame(
        [
            {
                "Region": "r1",
                "BANNER": "b1",
                "Store_Physical_Location_No": 1,
                "Department": "d1",
                "Section_Master": "s1",
                "SUM_CURR_MARGINS": 0,
                "SUM_CURR_SALES": 0,
                "SUM_OPT_MARGINS": 0,
                "SUM_OPT_SALES": 0,
                "SUM_CURR_WIDTH": 1,
                "SUM_OPT_WIDTH": 1,
            }
        ]
    )

    pdf = get_projected_sales_margin(
        df_opt=df_opt,
        df_clusters=df_clusters,
        elas_sales=elas_sales,
        elas_margins=elas_margins,
    )

    assert pdf.equals(pdf_expected)
