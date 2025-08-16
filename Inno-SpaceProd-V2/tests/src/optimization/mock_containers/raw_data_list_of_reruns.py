import pandas as pd

from spaceprod.src.optimization.data_objects import (
    DataObjectScopeGeneral,
)
from spaceprod.src.optimization.pre_process.data_objects import (
    RawDataListOfReruns,
)

# to make sure timestamps are created properly
Timestamp = pd._libs.tslibs.timestamps.Timestamp

"""
# how this mock container was generated:
from spaceprod.utils.space_context import context
from spaceprod.src.optimization.data_objects import DataContainer, DataObjectScopeSpecific,DataObjectScopeGeneral
run="dbfs:/mnt/blob/sobeys_space_prod/test_space_run_20220413_223324_227602"
context.reload_context_from_run_folder(run)
path_opt_data_containers = context.data.path("data_containers_parent_path")


store_ph= ["11215", "11225", "11255", "11286"]
df_store_no = context.data.read("location").filter(F.col("Store_Physical_Location_No").isin(store_ph)).select("STORE_NO").dropDuplicates()
store_no = [x["STORE_NO"] for x in df_store_no.collect()]

# for mock_raw_data_region_banner_dept_store_11211
data_container = DataContainer(scope=DataObjectScopeGeneral())
data = data_container.read(path_opt_data_containers, "RawDataListOfReruns")
cont_1 = container_to_code(data,list_mock_store_physical_location_no=store_ph, list_store_no=store_no, container_name="RawDataListOfReruns", var_name="raw_data_list_of_reruns")  # you need this function (similar to df_to_ddl)
print(cont_1)
"""

raw_data_list_of_reruns = RawDataListOfReruns(
    name="RawDataListOfReruns",
    pdf_list_of_reruns=pd.DataFrame(
        [
            {
                "M_Cluster": "2A",
                "Region_Desc": "ontario",
                "Section_Master": "Frozen Handhelds",
                "Banner": "SOBEYS",
                "Opt_Width_X_Facing": 130.12,
                "Store_Physical_Location_No": "11225",
                "Department": "Frozen",
            },
            {
                "M_Cluster": "2A",
                "Region_Desc": "ontario",
                "Section_Master": "Frozen Handhelds",
                "Banner": "SOBEYS",
                "Opt_Width_X_Facing": 128.32,
                "Store_Physical_Location_No": "11286",
                "Department": "Frozen",
            },
            {
                "M_Cluster": "2A",
                "Region_Desc": "ontario",
                "Section_Master": "Frozen Pizza",
                "Banner": "SOBEYS",
                "Opt_Width_X_Facing": 106.72000000000001,
                "Store_Physical_Location_No": "11225",
                "Department": "Frozen",
            },
            {
                "M_Cluster": "2A",
                "Region_Desc": "ontario",
                "Section_Master": "Frozen Pizza",
                "Banner": "SOBEYS",
                "Opt_Width_X_Facing": 104.92000000000002,
                "Store_Physical_Location_No": "11286",
                "Department": "Frozen",
            },
        ]
    ),
    scope=DataObjectScopeGeneral(),
)
