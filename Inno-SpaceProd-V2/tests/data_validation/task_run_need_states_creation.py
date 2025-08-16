"""
This is a pipeline-specific file that defines the shape of the
expected output data that is generated when we run the integration test
for this pipeline

**** NOTE ****
IF YOUR TESTS BREAK DURING VALIDATION HERE,
BEFORE CHANGING THESE DATA CONTRACTS MAKE SURE YOU KNOWLEDGE THAT THIS CHANGE
THAT YOU ARE ABOUT TO MAKE IS DUE TO VALID LOGIC CHANGE IN THE CODE AND
IS NOT ACCIDENTAL


this file is for the following pipeline:
    spaceprod.src.clustering.need_states_creation.main.task_run_need_states_creation

"""

from tests.data_validation.validator import DatasetValidator

from spaceprod.utils.imports import T


validator_pre_processing = DatasetValidator(
    dataset_id="pre_processing",
    dimensions=[
        "CUSTOMER_CARD_ID",
        "SECTION_MASTER",
        "ITEM_A",
        "ITEM_B",
        "REGION",
        "NATIONAL_BANNER_DESC",
    ],
    non_null_columns=[
        "CUSTOMER_CARD_ID",
        "ITEM_A",
        "ITEM_B",
        "SECTION_MASTER",
        "REGION",
        "NATIONAL_BANNER_DESC",
    ],
    schema=T.StructType(
        [
            T.StructField("CUSTOMER_CARD_ID", T.StringType(), True),
            T.StructField("ITEM_A", T.StringType(), True),
            T.StructField("ITEM_B", T.StringType(), True),
            T.StructField("SECTION_MASTER", T.StringType(), True),
            T.StructField("NATIONAL_BANNER_DESC", T.StringType(), True),
            T.StructField("REGION", T.StringType(), True),
        ]
    ),
)

validator_final_need_states = DatasetValidator(
    dataset_id="final_need_states",
    dimensions=[
        "SECTION_MASTER",
        "ITEM_NO",
        "REGION",
        "NATIONAL_BANNER_DESC",
    ],
    non_null_columns=[
        "NATIONAL_BANNER_DESC",
        "REGION",
        "SECTION_MASTER",
        "ITEM_NO",
        "need_state",
    ],
    schema=T.StructType(
        [
            T.StructField("ITEM_NO", T.StringType(), True),
            T.StructField("need_state", T.StringType(), True),
            T.StructField("ITEM_NAME", T.StringType(), True),
            T.StructField("ITEM_SK", T.StringType(), True),
            T.StructField("LVL5_NAME", T.StringType(), True),
            T.StructField("LVL5_ID", T.StringType(), True),
            T.StructField("LVL2_ID", T.StringType(), True),
            T.StructField("LVL3_ID", T.StringType(), True),
            T.StructField("LVL4_NAME", T.StringType(), True),
            T.StructField("LVL3_NAME", T.StringType(), True),
            T.StructField("LVL2_NAME", T.StringType(), True),
            T.StructField("CATEGORY_ID", T.StringType(), True),
            T.StructField("EXEC_ID", T.StringType(), True),
            T.StructField("REGION", T.StringType(), True),
            T.StructField("NATIONAL_BANNER_DESC", T.StringType(), True),
            T.StructField("SECTION_MASTER", T.StringType(), True),
            T.StructField("cannib_id", T.StringType(), True),
        ]
    ),
)

validator_final_dist_matrix = DatasetValidator(
    dataset_id="final_dist_matrix",
    dimensions=[
        "NATIONAL_BANNER_DESC",
        "REGION",
        "SECTION_MASTER",
        "ITEM_A",
        "ITEM_B",
    ],
    non_null_columns=[
        "NATIONAL_BANNER_DESC",
        "REGION",
        "SECTION_MASTER",
        "ITEM_A",
        "ITEM_B",
    ],
    schema=T.StructType(
        [
            T.StructField("ITEM_A", T.StringType(), True),
            T.StructField("ITEM_B", T.StringType(), True),
            T.StructField("VALUE", T.StringType(), True),
            T.StructField("EXEC_ID", T.StringType(), True),
            T.StructField("REGION", T.StringType(), True),
            T.StructField("SECTION_MASTER", T.StringType(), True),
            T.StructField("NATIONAL_BANNER_DESC", T.StringType(), True),
        ]
    ),
)
