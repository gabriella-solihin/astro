from tests.utils.spark_util import spark_tests as spark

DF_POG_DEPARTMENT_MAPPING_FILE = spark.createDataFrame(
    [
        {
            "Region": "Ontario",
            "Banner": "FOODLAND",
            "SECTION_MASTER": "Frozen Handhelds",
            "PM_LVL2_Tag": "Frozen Grocery",
            "PM_LVL3_Tag": "Frozen Ready Meals",
            "PM_LVL4_Mode": "Frozen Pizza",
            "Department": "Frozen",
            "In_Scope": "1",
            "Sum of Item_Count": "43",
        },
        {
            "Region": "Ontario",
            "Banner": "SOBEYS",
            "SECTION_MASTER": "Frozen Handhelds",
            "PM_LVL2_Tag": "Frozen Grocery",
            "PM_LVL3_Tag": "Frozen Ready Meals",
            "PM_LVL4_Mode": "Frozen Pizza",
            "Department": "Frozen",
            "In_Scope": "1",
            "Sum of Item_Count": "62",
        },
        {
            "Region": "Ontario",
            "Banner": "SOBEYS",
            "SECTION_MASTER": "Frozen Pizza",
            "PM_LVL2_Tag": "Frozen Grocery",
            "PM_LVL3_Tag": "Frozen Ready Meals",
            "PM_LVL4_Mode": "Frozen Pizza",
            "Department": "Frozen",
            "In_Scope": "1",
            "Sum of Item_Count": "84",
        },
    ]
)
