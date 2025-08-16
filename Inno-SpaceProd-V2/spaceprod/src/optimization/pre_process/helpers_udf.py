from typing import List, Tuple, Optional

import pandas as pd

from spaceprod.utils.data_helpers import backup_on_blob
from spaceprod.utils.imports import F, SparkDataFrame, T
from spaceprod.utils.names import get_col_names
from spaceprod.utils.space_context.spark import spark


def generate_region_banner_dept_tuples(
    pdf: pd.DataFrame,
) -> List[Tuple[str, str, str]]:
    n = get_col_names()
    region_banner_dept_tuple = list(
        tuple(x)
        for x in pdf[[n.F_REGION_DESC, n.F_BANNER, n.F_DEPARTMENT]]
        .drop_duplicates()
        .values
    )

    return region_banner_dept_tuple


def generate_region_banner_dept_skeleton(
    rerun: bool,
    pdf_elasticity_curves_sales: Optional[pd.DataFrame] = None,
    pdf_list_of_reruns: Optional[pd.DataFrame] = None,
) -> SparkDataFrame:
    """
    Generates a simple "skeleton" dataset containing only required dimensions:
    region / banner / dept.
    It is used downstream to join in the required attributes.
    This dataset dictates the scope of the execution.

    Parameters
    ----------
    rerun : bool
        rerun for margin option

    pdf_elasticity_curves_sales : pd.DataFrame
        a dataset that contains the right region / banner / dept scope for
        the general run

    pdf_list_of_reruns : pd.DataFrame
        a dataset that contains the right region / banner / dept scope for
        the margin rerun run

    Returns
    -------
    a simple "skeleton" dataset containing only required dimensions:
    region / banner / dept.
    """

    # which data to use for rerun
    pdf = pdf_list_of_reruns if rerun else pdf_elasticity_curves_sales

    msg = """
    If `rerun`=False, need to supply: `pdf_elasticity_curves_sales`
    If `rerun`=True, need to supply: `pdf_list_of_reruns`
    """

    assert pdf is not None, msg

    region_banner_dept_tuple = generate_region_banner_dept_tuples(pdf=pdf)

    schema = T.StructType(
        [
            T.StructField("REGION", T.StringType(), True),
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("DEPT", T.StringType(), True),
        ]
    )
    df_skeleton = spark.createDataFrame(region_banner_dept_tuple, schema=schema)
    df_skeleton = backup_on_blob(spark, df_skeleton)
    return df_skeleton


def generate_region_banner_dept_store_skeleton_explode_store(
    df_task_opt_pre_proc_region_banner_dept_step_two: SparkDataFrame,
    filter_for_specific_stores: bool,
):
    """
    Creates a dataset that serves as a "skeleton" dataset, i.e. data that
    contains only dimensions for future data. As the pipeline runs, new
    columns will be added / joined in.

    Parameters
    ----------
    df_task_opt_pre_proc_region_banner_dept_step_two: output of the
    upstream task

    Returns
    -------
    a skeleton dataset that contains region, banner, dept and store
    """

    from spaceprod.utils.space_context.spark import spark

    cols = [
        "REGION",
        "BANNER",
        "DEPT",
        F.explode("STORE_LIST").alias("STORE"),
    ]

    df = df_task_opt_pre_proc_region_banner_dept_step_two.select(*cols)

    if filter_for_specific_stores:
        # filter on only store in merged cluster 1C. 50 stores in total. based on new list 4/15
        target_stores = ["13604"]  # ,  #pilot store
        # "10798",
        # "10519",
        # "10556",
        # "10805",
        # "10709",
        # "13304",
        # "10803",
        # "12583",
        # "10764",
        # "10526",
        # "10179",
        # "10530",
        # "10518",
        # "12886",
        #     "19264",
        #     "16064",
        #     "14884",
        #     "14048",
        #     "10801",
        #     "10510",
        #     "12383",
        #     "14045",
        #     "10703",
        #     "13364",
        #     "12844",
        #     "10707",
        #     "17004",
        #     "17024",
        #     "19284",
        #     "10741",
        #     "10718",
        #     "14284",
        #     "12524",
        #     "19305",
        #     "10609",
        #     "20307",
        #     "21206",
        #     "14049",
        #     "10514",
        #     "13905",
        #     "13645",
        #     "15045",
        #     "21445",
        #     "21429",
        #     "21426",
        #     "10728",
        #     "15624",
        #     "10791",
        #     "10766",
        #     "17324",
        #     "23644",
        #     "14046",
        #     "10723",
        #     "10802",
        #     "15424",
        #     "16824",
        #     "16005",
        #     "16825",
        #     "14730",
        #     "14733",
        #     "14047",
        #     "10623",
        #     "19348",
        # ]
        df = df.filter(F.col("STORE").isin(*target_stores))
    df = backup_on_blob(spark, df)

    return df
