from tests.data_validation.validator import DatasetValidator as DataContract

from spaceprod.utils.imports import T

store_cluster_mapping = DataContract(
    dataset_id="store_cluster_mapping",
    dimensions=[
        "store_physical_location_id",
        "store_cluster_id",
    ],
    non_null_columns=[
        "store_cluster_id",
        "store_physical_location_id",
        "store_no",
        "region",
        "banner",
        "department",
        "category",
        "category_size",
        "size_uom",
        "segmentation_cluster",
        "segmentation_cluster_desc",
        "expected_publish_date",
        "run_name",
        "run_id",
        "run_datetime",
    ],
    schema=T.StructType(
        [
            T.StructField("store_physical_location_id", T.StringType(), True),
            T.StructField("store_no", T.StringType(), True),
            T.StructField("region", T.StringType(), True),
            T.StructField("banner", T.StringType(), True),
            T.StructField("department", T.StringType(), True),
            T.StructField("category", T.StringType(), True),
            T.StructField("category_size", T.IntegerType(), True),
            T.StructField("size_uom", T.StringType(), True),
            T.StructField("segmentation_cluster", T.StringType(), True),
            T.StructField("segmentation_cluster_desc", T.StringType(), True),
            T.StructField("store_cluster_id", T.StringType(), True),
            T.StructField("expected_publish_date", T.StringType(), True),
            T.StructField("run_name", T.StringType(), True),
            T.StructField("run_id", T.StringType(), True),
            T.StructField("run_datetime", T.StringType(), True),
        ]
    ),
    data_patterns={
        "store_physical_location_id": "^\d{2}(?:\d{3})?$",  # 2-5 -digit number
        "store_no": "^\d{1,4}$",
        "region": "(ontario)|(west)|(atlantic)|(quebec)",
        "banner": "(SOBEYS)|(FOODLAND)|(IGA)|(SAFEWAY)|(FRESH CO)|(THRIFTY FOODS)",
        "category_size": "^\d{1,3}$",  # 1- or 2- or 3- digit number
        "size_uom": "(D)|(F)",
        "segmentation_cluster": "^(\d{1})(\w{1})$",  # 1 digit followed by 1 letter
        "expected_publish_date": "^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$",
    },
)

# TODO: the below contract should have:
# dimensions=["store_cluster_id", "item_no", "category"]

micro = DataContract(
    dataset_id="system_micro_output",
    dimensions=["store_cluster_id", "item_no"],
    non_null_columns=[
        "store_cluster_id",
        "category",
        "sales_or_margin",
        "item_no",
        "needstate_idx",
        "ideal_adjacency",
        "optimal_facings",
        "reduce_rank",
        "increase_rank",
        "expected_publish_date",
        "run_name",
        "run_id",
        "run_datetime",
    ],
    schema=T.StructType(
        [
            T.StructField("store_cluster_id", T.StringType(), True),
            T.StructField("category", T.StringType(), True),
            T.StructField("sales_or_margin", T.StringType(), True),
            T.StructField("item_no", T.StringType(), True),
            T.StructField("needstate_idx", T.StringType(), True),
            T.StructField("ideal_adjacency", T.StringType(), True),
            T.StructField("optimal_facings", T.DoubleType(), True),
            T.StructField("reduce_rank", T.IntegerType(), True),
            T.StructField("increase_rank", T.IntegerType(), True),
            T.StructField("expected_publish_date", T.StringType(), True),
            T.StructField("run_name", T.StringType(), True),
            T.StructField("run_id", T.StringType(), True),
            T.StructField("run_datetime", T.StringType(), True),
        ]
    ),
    data_patterns={
        "item_no": "^(\d{2})|(\d{3})|(\d{4})|(\d{5})|(\d{6})$",  # 2-6 -digit number
        "sales_or_margin": "(sales)|(margin)",
        "needstate_idx": "^\d{1}(?:\d{1})?$",  # 1- or 2- digit number or a -1
        "ideal_adjacency": "^(\d{1})|(\d{2})|(\d{3})$",  # 1- or 2- or 3- digit number
        "optimal_facings": "^\d{1,2}$|^(?=\d+[.]\d+$).{3,4}$",
        "reduce_rank": "^(-)?(\d{1})|(\d{2})|(\d{3})$",  # 2-3 -digit number
        "increase_rank": "^(-)?(\d{1})|(\d{2})|(\d{3})$",  # 2-3 -digit number
        "expected_publish_date": "^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$",
    },
)

# TODO: the below contract should have:
# dimensions= ["store_physical_location_id", "segmentation_cluster", "category"]

macro = DataContract(
    dataset_id="system_macro_output",
    dimensions=["store_physical_location_id", "category"],
    non_null_columns=[
        "store_cluster_id",
        "store_physical_location_id",
        "store_no",
        "segmentation_cluster",
        "category",
        "department",
        "sales_or_margin",
        "logical_segment_size_ft",
        "size_uom",
        "optimal_size",
        "current_value",
        "optimal_value",
        "optimal_unique_items",
        "current_size",
        "current_facings",
        "current_unique_items",
        "reduce_rank",
        "increase_rank",
        "expected_publish_date",
        "run_name",
        "run_id",
        "run_datetime",
    ],
    schema=T.StructType(
        [
            T.StructField("store_cluster_id", T.StringType(), True),
            T.StructField("store_physical_location_id", T.StringType(), True),
            T.StructField("store_no", T.StringType(), True),
            T.StructField("segmentation_cluster", T.StringType(), True),
            T.StructField("category", T.StringType(), True),
            T.StructField("department", T.StringType(), True),
            T.StructField("sales_or_margin", T.StringType(), True),
            T.StructField("logical_segment_size_ft", T.LongType(), True),
            T.StructField("size_uom", T.StringType(), True),
            T.StructField("optimal_size", T.StringType(), True),
            T.StructField("optimal_value", T.StringType(), True),
            T.StructField("current_value", T.StringType(), True),
            T.StructField("optimal_unique_items", T.StringType(), True),
            T.StructField("current_size", T.StringType(), True),
            T.StructField("current_facings", T.StringType(), True),
            T.StructField("current_unique_items", T.StringType(), True),
            T.StructField("reduce_rank", T.IntegerType(), True),
            T.StructField("increase_rank", T.IntegerType(), True),
            T.StructField("expected_publish_date", T.StringType(), True),
            T.StructField("run_name", T.StringType(), True),
            T.StructField("run_id", T.StringType(), True),
            T.StructField("run_datetime", T.StringType(), True),
        ]
    ),
    data_patterns={
        "store_physical_location_id": "^\d{2}(?:\d{3})?$",  # 2-5 -digit number
        "store_no": "^\d{1,4}$",
        "segmentation_cluster": "^(\d{1})(\w{1})$",  # 1 digit followed by 1 letter
        "optimal_size": "^\d{1,4}$|^(?=\d+[.]\d+$).{3,9}$",  # ex:-121.04492
        "current_size": "^\d{1,4}$|^(?=\d+[.]\d+$).{3,9}$",  # ex:-121.04492
        "sales_or_margin": "(sales)|(margin)",
        "logical_segment_size_ft": "^\d{1}(?:\d{1})?$",  # 1-2 -digit number
        "current_unique_items": "^(\d{1})|(\d{2})|(\d{3})$",  # 1-3 -digit number
        "reduce_rank": "^(\d{1})|(\d{2})|(\d{3})$",  # 1-3 -digit number
        "increase_rank": "^(\d{1})|(\d{2})|(\d{3})$",  # 1-3 -digit number
        "size_uom": "(D)|(F)",
        "expected_publish_date": "^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$",
    },
)
