import pandas as pd
import pytest
from pyspark.sql import SparkSession

from tests.src.optimization.mock_containers.data_modelling_create_and_solve_model import (
    mock_data_modelling_create_and_solve_model_11215,
)
from tests.src.optimization.mock_containers.data_modelling_results_processing_per_reg_ban_dept import (
    data_modelling_results_processing_per_reg_ban_dept,
)
from tests.src.optimization.mock_containers.raw_data_general import (
    mock_raw_data_general,
)
from tests.src.optimization.mock_containers.raw_data_region_banner_dept_step_two import (
    raw_data_region_banner_dept_step_two,
)
from tests.src.optimization.mock_containers.raw_data_region_banner_dept_store import (
    mock_raw_data_region_banner_dept_store_11215,
)

from spaceprod.src.optimization.post_process.data_objects import (
    DataModellingConcatenatedResults,
)
from spaceprod.src.optimization.post_process.helpers import (
    concatenate_and_summarize_all_results,
)
from spaceprod.src.optimization.post_process.udf import (
    generate_udf_post_proc_concat,
    generate_udf_results_processing_per_reg_ban_dept,
)
from spaceprod.utils.data_helpers import get_temp_paths, write_blob


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    from tests.utils.spark_util import spark_tests as spark

    return spark


def test_udf_post_proc_per_reg_ban_dept(spark):

    # get temp path to store mock data
    path_partition_root = get_temp_paths(name="test_udf_post_proc_partitions")[0]

    data_map = {
        "RawDataRegionBannerDeptStore": {
            "elasticity_curves": {
                "cols": [
                    "Section_Master",
                    "Need_State",
                    "Need_State_Idx",
                    "Item_No",
                    "Item_Name",
                    "M_Cluster",
                    "Facings",
                    "Sales",
                    "REGION_DIM",
                    "BANNER_DIM",
                    "DEPT_DIM",
                    "STORE_DIM",
                ],
                "path": f"{path_partition_root}/elasticity_curves",
            },
            "shelve_space_df": {
                "cols": [
                    "Store_No",
                    "Item_No",
                    "Section_Name",
                    "Release_Date",
                    "Facings",
                    "Length_Section_Inches",
                    "Width",
                    "Section_Master",
                    "Banner",
                    "Region_Desc",
                    "Department",
                    "Store_Physical_Location_No",
                    "Legal_Increment_Width",
                    "Legal_Breaks_In_Section",
                    "REGION_DIM",
                    "BANNER_DIM",
                    "DEPT_DIM",
                    "STORE_DIM",
                ],
                "path": f"{path_partition_root}/shelve_space_df",
            },
            "product_info": {
                "cols": [
                    "Item_No",
                    "Width",
                    "Item_Name",
                    "Lvl5_Name",
                    "Lvl4_Name",
                    "Lvl3_Name",
                    "Lvl2_Name",
                    "Section_Master",
                    "Need_State",
                    "Constant_Treatment",
                    "dupes",
                    "Store_Physical_Location_No",
                    "Facings",
                    "Cur_Width_X_Facing",
                    "REGION_DIM",
                    "BANNER_DIM",
                    "DEPT_DIM",
                    "STORE_DIM",
                ],
                "path": f"{path_partition_root}/product_info",
            },
            "merged_clusters_df": {
                "cols": [
                    "Store_Physical_Location_No",
                    "Store_No",
                    "M_Cluster",
                    "REGION_DIM",
                    "BANNER_DIM",
                    "DEPT_DIM",
                    "STORE_DIM",
                ],
                "path": f"{path_partition_root}/merged_clusters_df",
            },
            "store_category_dims": {
                "cols": [
                    "Store_No",
                    "Store_Physical_Location_No",
                    "M_Cluster",
                    "Section_Name",
                    "Section_Master",
                    "Cur_Width_X_Facing",
                    "Length_Section_Optimizable",
                    "Length_Section_Optimizable_lower",
                    "max_facings_in_opt",
                    "Legal_Breaks_In_Section",
                    "Legal_Increment_Width",
                    "Lin_Space_Per_Break",
                    "No_Of_Shelves",
                    "Theoretic_lin_space",
                    "Cur_Section_Width_Deviation",
                    "Local_Item_Width",
                    "REGION_DIM",
                    "BANNER_DIM",
                    "DEPT_DIM",
                    "STORE_DIM",
                ],
                "path": f"{path_partition_root}/store_category_dims",
            },
        },
        "DataModellingCreateAndSolveModel": {
            "opt_x_df": {
                "cols": [
                    "Item_No",
                    "Facings",
                    "Store_Physical_Location_No",
                    "solution_value",
                    "REGION_DIM",
                    "BANNER_DIM",
                    "DEPT_DIM",
                    "STORE_DIM",
                ],
                "path": f"{path_partition_root}/opt_x_df",
            },
            "opt_q_df": {
                "cols": [
                    "index",
                    "Store_Physical_Location_No",
                    "solution_value",
                    "REGION_DIM",
                    "BANNER_DIM",
                    "DEPT_DIM",
                    "STORE_DIM",
                ],
                "path": f"{path_partition_root}/opt_q_df",
            },
        },
    }

    # here we use 3 mock containers for 4 different stores and
    # store them in a temporary folder in order for the udf logic to access
    # it

    # first setup the temp folder
    path = get_temp_paths(name="test_udf_post_proc_per_reg_ban_dept")[0]

    # RawDataGeneral container (NOT store-specific)
    mock_raw_data_general.save(path)

    # RawDataRegionBannerDeptStore container (store-by-store)
    mock_raw_data_region_banner_dept_store_11215.save(path)

    # DataModellingCreateAndSolveModel (store-by-store)
    mock_data_modelling_create_and_solve_model_11215.save(path)

    # RawDataRegionBannerDeptStepTwo (region/banner/dept)
    raw_data_region_banner_dept_step_two.save(path)

    # mock PDF data to be used in UDF
    pdf = pd.DataFrame(
        [
            {
                "REGION": "ontario",
                "BANNER": "SOBEYS",
                "DEPT": "Frozen",
                "STORE": "11215",
            }
        ]
    )

    # we collect arguments to be passed to

    for container_name, container_datasets in data_map.items():
        for dataset_id, dataset_info in container_datasets.items():

            # test the udf where we concat data

            udf_to_test = generate_udf_post_proc_concat(
                path_opt_data_containers=path,
                container_name=container_name,
                dataset_name_from_the_container=dataset_id,
            )

            # call the udf underlying function
            # NOTE: we are not calling the udf using the standard Spark way
            # we are just calling the underlying function
            pdf_res = udf_to_test.func(pdf)

            cols_act = list(pdf_res.columns)
            cols_exp = dataset_info["cols"]
            cols_miss = list(set(cols_exp) - set(cols_act))
            cols_extra = list(set(cols_act) - set(cols_exp))

            msg = f"""
            in container '{container_name}'
            in dataset '{dataset_id}'
            missing columns: {cols_miss}
            extra cols: {cols_extra}
            """

            assert set(cols_exp) == set(cols_act), msg

            # we will save the data here to be able to use for test
            # for the next usd
            df_res = spark.createDataFrame(pdf_res)

            dims = ["REGION_DIM", "BANNER_DIM", "DEPT_DIM"]

            write_blob(
                spark=spark,
                df=df_res,
                path=dataset_info["path"],
                partition_by=dims,
            )

    # now test the udf where we do post-processing

    # get the required paths to partitioned parquet
    data_map_raw = data_map["RawDataRegionBannerDeptStore"]
    data_map_mod = data_map["DataModellingCreateAndSolveModel"]
    path_elasticity_curves = data_map_raw["elasticity_curves"]["path"]
    path_shelve_space_df = data_map_raw["shelve_space_df"]["path"]
    path_product_info = data_map_raw["product_info"]["path"]
    path_merged_clusters_df = data_map_raw["merged_clusters_df"]["path"]
    path_store_category_dims = data_map_raw["store_category_dims"]["path"]
    path_opt_x_df = data_map_mod["opt_x_df"]["path"]
    path_opt_q_df = data_map_mod["opt_q_df"]["path"]

    # generated the udf to run
    udf_to_test = generate_udf_results_processing_per_reg_ban_dept(
        path_opt_data_containers=path,
        path_elasticity_curves=path_elasticity_curves,
        path_shelve_space_df=path_shelve_space_df,
        path_product_info=path_product_info,
        path_merged_clusters_df=path_merged_clusters_df,
        path_store_category_dims=path_store_category_dims,
        path_opt_x_df=path_opt_x_df,
        path_opt_q_df=path_opt_q_df,
        rerun=False,
        dependent_var="Sales",
        difference_of_facings_to_allocate_in_unit_proportions=0,
        max_facings=13,
        enable_supplier_own_brands_constraints=True,
    )

    # mock PDF data to be used in UDF
    pdf = pd.DataFrame(
        [
            {
                "REGION": "ontario",
                "BANNER": "SOBEYS",
                "DEPT": "Frozen",
            }
        ]
    )

    pdf_res = udf_to_test.func(pdf)

    # some basic assertions

    cols_new = ["PATH_MODELLING_RESULTS_PROCESSING_PER_REG_BAN_DEPT", "LIST_STORES"]
    cols = list(pdf.columns) + cols_new
    assert set(pdf_res.columns) == set(cols), "wrong cols"

    assert len(pdf_res) == 1

    val_act = pdf_res["PATH_MODELLING_RESULTS_PROCESSING_PER_REG_BAN_DEPT"][0]
    val_exp = "DataModellingResultsProcessingPerRegBanDept/ontario/SOBEYS/Frozen.pkl"
    assert val_act == val_exp

    # TODO:
    #  this test is setup as a unit test, but it tests the entire UDF logic in
    #  Opt post-processing as a single unit. It does not unit-test the
    #  individual helper functions, which ideally needs to be done as well.
    #  All of those individual helper functions run as part of this test
    #  but those are not technically unit tested. This is more of a
    #  mini-integration test for the entire Opt post-processing.

    # TODO:
    #  we are not testing for Margin / True scenario
    #  use pytest.mark.parametrize decorator to parametrize the 2 test cases:
    #  - Sales / False
    #  - Margin / True
    #  NOTE: you will need to implement the 'rerun' version of all the
    #  containers


def test_concatenate_and_summarize_all_results():
    # first setup the temp folder
    path = get_temp_paths(name="test_udf_post_proc_per_reg_ban_dept")[0]

    region_banner_dept_tuple = [("ontario", "SOBEYS", "Frozen")]

    data_modelling_results_processing_per_reg_ban_dept.save(path)

    data: DataModellingConcatenatedResults = concatenate_and_summarize_all_results(
        path_opt_data_containers=path,
        region_banner_dept_tuple=region_banner_dept_tuple,
        dependent_var="Sales",
        rerun=False,
    )

    assert isinstance(data, DataModellingConcatenatedResults)
