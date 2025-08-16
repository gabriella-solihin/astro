import pytest
import os
from pathlib import Path


from spaceprod.utils.config_helpers import parse_config


@pytest.fixture(scope="session")
def config():
    current_file_path = Path(__file__).resolve()

    config_test_path = os.path.join(current_file_path.parents[3], "tests", "config")
    global_config_path = os.path.join(config_test_path, "spaceprod_config.yaml")

    config_path = os.path.join(
        config_test_path, "optimization", "micro_optimization_config.yaml"
    )

    config = parse_config([global_config_path, config_path])
    config["pipeline_steps"]["optimization"]["micro"] = True
    return config


@pytest.mark.skip  # TODO: address: https://dev.azure.com/SobeysInc/Inno-SpaceProd/_TestManagement/Runs?runId=108442
def test_micro_optimization_pipeline(config):

    from spaceprod.src.optimization.model_handling.model_handling_micro import (
        create_and_solve_micro_model,
    )
    from spaceprod.src.optimization.results_processing.results_processing_micro import (
        micro_results_processing,
    )

    data = process_all_micro_raw_data(config)
    assert type(data[0]).__name__ == "tuple"
    assert data[0][1].startswith("CANNIB")

    cat = data[1][1]
    store = int(data[1][0])
    sets_output = construct_all_micro_sets(config, get_col_names(), log, store, cat)
    assert type(sets_output.facings).__name__ == "set"
    assert max(sets_output.facings) == config["parameters"]["max_facings_per_item"]
    assert type(sets_output.items).__name__ == "set"

    model_results = create_and_solve_micro_model(
        config, get_col_names(), log, store, cat, write=True
    )
    assert set(model_results.opt_x_df.columns) == set(
        [
            "Item_No",
            "Facings",
            "Store_Physical_Location_No",
            "Cannib_Id",
            "solution_value",
        ]
    )
    assert set(model_results.opt_y_df.columns) == set(
        [
            "index",
            "Item_No",
            "Store_Physical_Location_No",
            "Cannib_Id",
            "solution_value",
        ]
    )

    summary_per_store_and_cat, item_output = micro_results_processing(
        cfg=config, n=get_col_names(), log=log, store_category_tuple=[data[1]]
    )

    assert set(summary_per_store_and_cat.columns) == set(
        [
            "M_Cluster",
            "Store_Physical_Location_No",
            "Cannib_Id",
            "Cur_Sales",
            "Cur_Width_x_Facing",
            "Current_Facings",
            "Opt_Sales",
            "Opt_Width_x_Facing",
            "Optim_Facings",
            "Unique_Items_Cur_Assorted",
            "Unique_Items_Opt_Assorted",
            "Cur_Sales_per_feet",
            "Opt_Sales_per_feet",
            "Cur_avg_Sales_per_facing",
            "Opt_avg_Sales_per_facing",
            "Sales_cur_opt_delta",
            "Store_No",
        ]
    )

    assert set(item_output.columns) == set(
        [
            "Region_Desc",
            "M_Cluster",
            "Store_Physical_Location_No",
            "Cannib_Id",
            "Lvl4_Name",
            "Need_State",
            "Need_State_Idx",
            "Item_No",
            "Item_Name",
            "Width",
            "Constant_Treatment",
            "Current_Facings",
            "Optim_Facings",
            "Cur_Sales",
            "Opt_Sales",
            "Unique_Items_Cur_Assorted",
            "Unique_Items_Opt_Assorted",
            "Cur_Width_x_Facing",
            "Opt_Width_x_Facing",
            "Cur_Opt_Same_Facing",
            "Opt_Minus_Cur_Facings",
        ]
    )
