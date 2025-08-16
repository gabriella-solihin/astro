import pandas as pd

from spaceprod.src.optimization.data_objects import (
    DataObjectScopeSpecific,
)


# to make sure timestamps are created properly
from spaceprod.src.optimization.modelling.data_objects import (
    DataModellingCreateAndSolveModel,
)
from spaceprod.src.optimization.modelling.helpers_model_handling import (
    ModelResults,
)

Timestamp = pd._libs.tslibs.timestamps.Timestamp
"""
# how this mock container was generated:
run="dbfs:/mnt/blob/sobeys_space_prod/test_space_run_20220413_223324_227602"
context.reload_context_from_run_folder(run)
path_opt_data_containers = context.data.path("data_containers_parent_path")

store_ph= ["11215", "11225", "11255", "11286"]
df_store_no = context.data.read("location").filter(F.col("Store_Physical_Location_No").isin(store_ph)).select("STORE_NO").dropDuplicates()
store_no = [x["STORE_NO"] for x in df_store_no.collect()]

# for mock_data_modelling_create_and_solve_model_11211
data_container = DataContainer(scope=DataObjectScopeSpecific(region="ontario",banner="SOBEYS",dept="Frozen",store=store_ph[0]))
data = data_container.read(path_opt_data_containers, "DataModellingCreateAndSolveModel")
store_1 = container_to_code(data,list_mock_store_physical_location_no=store_ph, list_store_no=store_no, container_name='DataModellingCreateAndSolveModel', var_name='mock_data_modelling_create_and_solve_model_'+store_ph[0])  # you need this function (similar to df_to_ddl)

# for mock_data_modelling_create_and_solve_model_11220
data_container = DataContainer(scope=DataObjectScopeSpecific(region="ontario",banner="SOBEYS",dept="Frozen",store=store_ph[1]))
data = data_container.read(path_opt_data_containers, "DataModellingCreateAndSolveModel")
store_2 = container_to_code(data,list_mock_store_physical_location_no=store_ph, list_store_no=store_no, container_name='DataModellingCreateAndSolveModel', var_name='mock_data_modelling_create_and_solve_model_'+store_ph[1])  # you need this function (similar to df_to_ddl)

# for mock_data_modelling_create_and_solve_model_12083
data_container = DataContainer(scope=DataObjectScopeSpecific(region="ontario",banner="SOBEYS",dept="Frozen",store=store_ph[2]))
data = data_container.read(path_opt_data_containers, "DataModellingCreateAndSolveModel")
store_3 = container_to_code(data,list_mock_store_physical_location_no=store_ph, list_store_no=store_no, container_name='DataModellingCreateAndSolveModel', var_name='mock_data_modelling_create_and_solve_model_'+store_ph[2])  # you need this function (similar to df_to_ddl)

# for mock_data_modelling_create_and_solve_model_18205
data_container = DataContainer(scope=DataObjectScopeSpecific(region="ontario",banner="SOBEYS",dept="Frozen",store=store_ph[3]))
data = data_container.read(path_opt_data_containers, "DataModellingCreateAndSolveModel")
store_4 = container_to_code(data,list_mock_store_physical_location_no=store_ph, list_store_no=store_no, container_name='DataModellingCreateAndSolveModel', var_name='mock_data_modelling_create_and_solve_model_'+store_ph[3])  # you need this function (similar to df_to_ddl)
print(f"\n{store_1}\n{store_2}\n{store_3}\n{store_4}\n")
"""

mock_data_modelling_create_and_solve_model_11215 = DataModellingCreateAndSolveModel(
    name="DataModellingCreateAndSolveModel",
    model_results=ModelResults(
        opt_x_df=pd.DataFrame(
            [
                {
                    "Item_No": "314792",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "314792",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "314792",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "541805",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "541805",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "541805",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "633928",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "633928",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "633928",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "164849",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "164849",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "164849",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "955963",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "955963",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "955963",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "819649",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "819649",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "819649",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "375044",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375044",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375044",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "678984",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "678984",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "678984",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "576782",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "576782",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "576782",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "900100",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "900100",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "900100",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "375074",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375074",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "375074",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "265836",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "265836",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "265836",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "413819",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "413819",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "413819",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "375039",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375039",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375039",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 1.0,
                },
            ]
        ),
        opt_q_df=pd.DataFrame(
            [
                {
                    "index": "Frozen Pizza",
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 4.0,
                },
                {
                    "index": "Frozen Handhelds",
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 4.0,
                },
            ]
        ),
        opt_l_df=pd.DataFrame(
            [
                {
                    "index": ("Frozen Pizza", "-1", "314792"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "-1", "900100"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "-1", "164849"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "1", "955963"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "1", "375039"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "819649"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "678984"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375044"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "413819"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375074"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "0", "576782"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "0", "633928"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "1", "265836"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "1", "541805"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
            ]
        ),
        opt_u_df=pd.DataFrame(
            [
                {
                    "index": ("Frozen Pizza", "-1", "314792"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "-1", "900100"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "-1", "164849"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "1", "955963"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "1", "375039"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "819649"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "678984"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375044"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "413819"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375074"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "0", "576782"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "0", "633928"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "1", "265836"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "1", "541805"),
                    "Store_Physical_Location_No": "11215",
                    "solution_value": 0.0,
                },
            ]
        ),
    ),
    scope=DataObjectScopeSpecific(
        region="ontario",
        banner="SOBEYS",
        dept="Frozen",
        store="11215",
    ),
)

mock_data_modelling_create_and_solve_model_11225 = DataModellingCreateAndSolveModel(
    name="DataModellingCreateAndSolveModel",
    model_results=ModelResults(
        opt_x_df=pd.DataFrame(
            [
                {
                    "Item_No": "541805",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "541805",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "541805",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "955963",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "955963",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "955963",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "678984",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "678984",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "678984",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "375044",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375044",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375044",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "819649",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "819649",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "819649",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "576782",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "576782",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "576782",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "375074",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375074",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375074",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "413819",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "413819",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "413819",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "375039",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375039",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375039",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 1.0,
                },
            ]
        ),
        opt_q_df=pd.DataFrame(
            [
                {
                    "index": "Frozen Pizza",
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 7.0,
                },
                {
                    "index": "Frozen Handhelds",
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 7.0,
                },
            ]
        ),
        opt_l_df=pd.DataFrame(
            [
                {
                    "index": ("Frozen Pizza", "1", "955963"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "1", "375039"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "678984"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "819649"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375044"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "413819"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375074"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "0", "576782"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "1", "541805"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
            ]
        ),
        opt_u_df=pd.DataFrame(
            [
                {
                    "index": ("Frozen Pizza", "1", "955963"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "1", "375039"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "678984"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "819649"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375044"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "413819"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375074"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "0", "576782"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "1", "541805"),
                    "Store_Physical_Location_No": "11225",
                    "solution_value": 0.0,
                },
            ]
        ),
    ),
    scope=DataObjectScopeSpecific(
        region="ontario",
        banner="SOBEYS",
        dept="Frozen",
        store="11225",
    ),
)

mock_data_modelling_create_and_solve_model_11255 = DataModellingCreateAndSolveModel(
    name="DataModellingCreateAndSolveModel",
    model_results=ModelResults(
        opt_x_df=pd.DataFrame(
            [
                {
                    "Item_No": "314792",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "314792",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "314792",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "541805",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "541805",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "541805",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "633928",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "633928",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "633928",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "164849",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "164849",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "164849",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "955963",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "955963",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "955963",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "819649",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "819649",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "819649",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "375044",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375044",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375044",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "678984",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "678984",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "678984",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "576782",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "576782",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "576782",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "900100",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "900100",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "900100",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "375074",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375074",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "375074",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "265836",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "265836",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "265836",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "413819",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "413819",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "413819",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "375039",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375039",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375039",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 1.0,
                },
            ]
        ),
        opt_q_df=pd.DataFrame(
            [
                {
                    "index": "Frozen Pizza",
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 6.0,
                },
                {
                    "index": "Frozen Handhelds",
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 6.0,
                },
            ]
        ),
        opt_l_df=pd.DataFrame(
            [
                {
                    "index": ("Frozen Pizza", "-1", "314792"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "-1", "900100"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "-1", "164849"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "1", "955963"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "1", "375039"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "819649"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "678984"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375044"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "413819"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375074"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "0", "576782"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "0", "633928"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "1", "265836"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "1", "541805"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
            ]
        ),
        opt_u_df=pd.DataFrame(
            [
                {
                    "index": ("Frozen Pizza", "-1", "314792"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "-1", "900100"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "-1", "164849"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "1", "955963"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "1", "375039"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "819649"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "678984"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375044"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "413819"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375074"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "0", "576782"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "0", "633928"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "1", "265836"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "1", "541805"),
                    "Store_Physical_Location_No": "11255",
                    "solution_value": 0.0,
                },
            ]
        ),
    ),
    scope=DataObjectScopeSpecific(
        region="ontario",
        banner="SOBEYS",
        dept="Frozen",
        store="11255",
    ),
)

mock_data_modelling_create_and_solve_model_11286 = DataModellingCreateAndSolveModel(
    name="DataModellingCreateAndSolveModel",
    model_results=ModelResults(
        opt_x_df=pd.DataFrame(
            [
                {
                    "Item_No": "541805",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "541805",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "541805",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "955963",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "955963",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "955963",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "678984",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "678984",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "678984",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "375044",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375044",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375044",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "819649",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "819649",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "819649",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "576782",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "576782",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "576782",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "375074",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375074",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375074",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "413819",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "413819",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "413819",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 1.0,
                },
                {
                    "Item_No": "375039",
                    "Facings": 0,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375039",
                    "Facings": 1,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "Item_No": "375039",
                    "Facings": 2,
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 1.0,
                },
            ]
        ),
        opt_q_df=pd.DataFrame(
            [
                {
                    "index": "Frozen Pizza",
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 6.0,
                },
                {
                    "index": "Frozen Handhelds",
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 6.0,
                },
            ]
        ),
        opt_l_df=pd.DataFrame(
            [
                {
                    "index": ("Frozen Pizza", "1", "955963"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "1", "375039"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "678984"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "819649"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375044"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "413819"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375074"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "0", "576782"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "1", "541805"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
            ]
        ),
        opt_u_df=pd.DataFrame(
            [
                {
                    "index": ("Frozen Pizza", "1", "955963"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "1", "375039"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "678984"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Pizza", "2", "819649"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375044"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "413819"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "-1", "375074"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "0", "576782"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
                {
                    "index": ("Frozen Handhelds", "1", "541805"),
                    "Store_Physical_Location_No": "11286",
                    "solution_value": 0.0,
                },
            ]
        ),
    ),
    scope=DataObjectScopeSpecific(
        region="ontario",
        banner="SOBEYS",
        dept="Frozen",
        store="11286",
    ),
)
