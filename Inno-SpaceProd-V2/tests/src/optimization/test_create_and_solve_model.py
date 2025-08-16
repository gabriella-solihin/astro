import pandas as pd
from spaceprod.src.optimization.modelling.udf import (
    generate_udf_create_and_solve_model,
)

from spaceprod.utils.data_helpers import get_temp_paths
from tests.src.optimization.mock_containers.data_modelling_construct_all_sets import (
    mock_data_modelling_construct_all_sets_11215,
)


def test_create_and_solve_model():
    # here we use DataModellingConstructAllSets mock container for 1
    # store (11211) and store it in a temporary
    # folder in order for the udf logic to access it

    # first setup the temp folder
    path = get_temp_paths(name="test_udf_post_proc_per_reg_ban_dept")[0]

    # RawDataRegionBannerDeptStore container (store-by-store)
    mock_data_modelling_construct_all_sets_11215.save(path)

    # mock PDF data to be used in UDF
    pdf = pd.DataFrame(
        {
            "REGION": ["ontario"],
            "BANNER": ["SOBEYS"],
            "DEPT": ["Frozen"],
            "STORE": ["11215"],
        }
    )

    udf = generate_udf_create_and_solve_model(
        path_opt_data_containers=path,
        rerun=False,
        mip_gap=0.005,
        max_seconds=60,
        keep_files=False,
        threads=5,
        retry_seconds=120,
        large_number=99999,
        enable_supplier_own_brands_constraints=True,
        enable_sales_penetration=False,
        solver="CBC",
        mip_focus=None,
    )

    pdf_res = udf.func(pdf)

    # some basic assertions

    cols_new = [
        "PATH_MODELLING_CREATE_AND_SOLVE_MODEL",
        "OPTIMALITY_GAP",
        "MODEL_STATUS",
        "SOLVE_MODEL_RUN_TIME_DURATION",
        "SOLVE_MODEL_ATTEMPTS",
        "NUM_ITEMS",
        "MAX_SECONDS",
        "RETRY_SECONDS",
        "MIP_GAP",
        "THREADS",
        "IS_RERUN",
        "TRACEBACK",
        "OBJECTIVE",
    ]

    cols = list(pdf.columns) + cols_new
    assert set(pdf_res.columns) == set(cols), "wrong cols"

    assert len(pdf_res) == 1

    val_act = pdf_res["PATH_MODELLING_CREATE_AND_SOLVE_MODEL"][0]
    val_exp = "DataModellingCreateAndSolveModel/ontario/SOBEYS/Frozen/11215.pkl"
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
