import pandas as pd

from spaceprod.src.optimization.modelling.udf import (
    generate_udf_construct_all_sets,
)

from spaceprod.utils.data_helpers import get_temp_paths
from tests.src.optimization.mock_containers.raw_data_region_banner_dept_store import (
    mock_raw_data_region_banner_dept_store_11215,
)


def test_construct_all_sets():
    # here we use RawDataRegionBannerDeptStore mock container for 1
    # store (11211) and store it in a temporary
    # folder in order for the udf logic to access it

    # first setup the temp folder
    path = get_temp_paths(name="test_udf_post_proc_per_reg_ban_dept")[0]

    # RawDataRegionBannerDeptStore container (store-by-store)
    mock_raw_data_region_banner_dept_store_11215.save(path)

    # mock PDF data to be used in UDF
    pdf = pd.DataFrame(
        {
            "REGION": ["ontario"],
            "BANNER": ["SOBEYS"],
            "DEPT": ["Frozen"],
            "STORE": ["11215"],
        }
    )

    mock_legal_section_break_dict = {
        "use_legal_section_break_dict": False,
        "Frozen": {
            "legal_break_width": 30,
            "extra_space_in_legal_breaks_max": 2,
            "extra_space_in_legal_breaks_min": 2,
            "min_breaks_per_section": 1,
        },
        "Dairy": {
            "legal_break_width": 48,
            "extra_space_in_legal_breaks_max": 2,
            "extra_space_in_legal_breaks_min": 2,
            "min_breaks_per_section": 1,
        },
        "All_other": {
            "legal_break_width": 12,
            "extra_space_in_legal_breaks_max": 8,
            "extra_space_in_legal_breaks_min": 8,
            "min_breaks_per_section": 4,
        },
    }

    udf = generate_udf_construct_all_sets(
        path_opt_data_containers=path,
        dependent_var="Sales",
        max_facings=13,
        extra_space_in_legal_breaks_min=2,
        extra_space_in_legal_breaks_max=2,
        unit_proportions_tolerance_in_opt=0.1,
        filter_for_test_negotiations=False,
        unit_proportions_tolerance_in_opt_sales_penetration=0.05,
        rerun=False,
        legal_section_break_dict=mock_legal_section_break_dict,
    )

    # run the test
    pdf_res = udf.func(pdf)

    # some basic assertions

    col_new = "PATH_MODELLING_CONSTRUCT_ALL_SETS"
    cols = list(pdf.columns) + [col_new]
    assert set(pdf_res.columns) == set(cols), "wrong cols"

    assert len(pdf_res) == 1

    val_act = pdf_res[col_new][0]
    val_exp = "DataModellingConstructAllSets/ontario/SOBEYS/Frozen/11215.pkl"
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
