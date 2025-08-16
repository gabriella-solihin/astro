"""
This file contains DataContract objects for various datasets: EXTERNAL ONLY!
External datasets will be validated automatically upon context.data.read(...)
call if the dataset_id that is being read matches one of the variable names
defined here
NOTE: the Dataset ID of the external dataset must match the object instance
(variable name) here in order for it to be validated automatically.
"""
from tests.data_validation.validator import DatasetValidator as DataContract

from spaceprod.utils.imports import T

merged_clusters_external = DataContract(
    dataset_id="merged_clusters_external",
    allow_extra_columns=True,
    dimensions=[
        "STORE_PHYSICAL_LOCATION_NO",
    ],
    non_null_columns="all_columns",
    schema=T.StructType(
        [
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("REGION", T.StringType(), True),
            T.StructField("STORE_PHYSICAL_LOCATION_NO", T.StringType(), True),
            T.StructField("MERGE_CLUSTER", T.StringType(), True),
        ]
    ),
    data_patterns={
        "STORE_PHYSICAL_LOCATION_NO": "^\d{2}(?:\d{3})?$",  # 2-5 -digit number
        "REGION": "(ONTARIO)|(WEST)|(ATLANTIC)|(QUEBEC)",
        "BANNER": "(SOBEYS)|(FOODLAND)|(IGA)|(SAFEWAY)|(FRESH CO)|(THRIFTY FOODS)",
        "MERGE_CLUSTER": "^(\d{1})(\w{1})$",  # 1 digit followed by 1 letter
    },
)


region_banner_clusters_for_margin_reruns = DataContract(
    dataset_id="region_banner_clusters_for_margin_reruns",
    dimensions=[
        "BANNER",
        "REGION",
        "MERGE_CLUSTER",
    ],
    non_null_columns="all_columns",
    schema=T.StructType(
        [
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("REGION", T.StringType(), True),
            T.StructField("MERGE_CLUSTER", T.StringType(), True),
        ]
    ),
    data_patterns={
        "BANNER": "(SOBEYS)|(FOODLAND)|(IGA)|(SAFEWAY)|(FRESH CO)|(THRIFTY FOODS)",
        "REGION": "(ONTARIO)|(WEST)|(ATLANTIC)|(QUEBEC)",
        "MERGE_CLUSTER": "^(\d{1})(\w{1})$",  # 1 digit followed by 1 letter
    },
)


pog_department_mapping_file = DataContract(
    dataset_id="pog_department_mapping_file",
    dimensions=["Region", "Banner", "SECTION_MASTER"],
    non_null_columns=["In_Scope", "Department", "Region", "Banner", "SECTION_MASTER"],
    schema=T.StructType(
        [
            T.StructField("Region", T.StringType(), True),
            T.StructField("Banner", T.StringType(), True),
            T.StructField("SECTION_MASTER", T.StringType(), True),
            T.StructField("PM_LVL2_Tag", T.StringType(), True),
            T.StructField("PM_LVL3_Tag", T.StringType(), True),
            T.StructField("PM_LVL4_Mode", T.StringType(), True),
            T.StructField("Department", T.StringType(), True),
            T.StructField("In_Scope", T.StringType(), True),
            T.StructField("Sum of Item_Count", T.StringType(), True),
        ]
    ),
    data_patterns={
        "Banner": "(SOBEYS)|(FOODLAND)|(IGA)|(SAFEWAY)|(FRESH CO)|(THRIFTY FOODS)",
        "Region": "(Ontario)|(West)|(Atlantic)|(Quebec)",
    },
)


supp_own_brands_request = DataContract(
    dataset_id="supp_own_brands_request",
    dimensions="contains_full_duplicates",
    non_null_columns=["Own_Brands_Flag"],
    schema=T.StructType(
        [
            T.StructField("Region_Desc", T.StringType(), True),
            T.StructField("Banner", T.StringType(), True),
            T.StructField("Section_Master", T.StringType(), True),
            T.StructField("Supplier_Id", T.StringType(), True),
            T.StructField("Own_Brands_Flag", T.StringType(), True),
            T.StructField("Percentage_Space", T.StringType(), True),
        ]
    ),
)
