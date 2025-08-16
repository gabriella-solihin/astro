from tests.utils.spark_util import spark_tests as spark

from pyspark import Row

DF_LOCALIZED_SPACE_REQUEST = spark.createDataFrame(
    [
        Row(
            Region_Desc="Atlantic",
            Banner="Sobeys",
            Section_Master="Frozen Breakfast",
            Percentage_Space="0.5",
        ),
        Row(
            Region_Desc="Ontario",
            Banner="Sobeys",
            Section_Master="Frozen Breakfast",
            Percentage_Space="0.1",
        ),
        Row(
            Region_Desc="West",
            Banner="Sobeys",
            Section_Master="Frozen Breakfast",
            Percentage_Space="0.1",
        ),
        Row(
            Region_Desc="West",
            Banner="Safeway",
            Section_Master="Frozen Breakfast",
            Percentage_Space="0.06",
        ),
        Row(
            Region_Desc="Ontario",
            Banner="FreshCo",
            Section_Master="Frozen Breakfast",
            Percentage_Space="0.1",
        ),
        Row(
            Region_Desc="West",
            Banner="FreshCo",
            Section_Master="Frozen Breakfast",
            Percentage_Space="0.05",
        ),
        Row(
            Region_Desc="Atlantic",
            Banner="Foodland",
            Section_Master="Frozen Breakfast",
            Percentage_Space="0.05",
        ),
        Row(
            Region_Desc="Ontario",
            Banner="Foodland",
            Section_Master="Frozen Breakfast",
            Percentage_Space="0.06",
        ),
        Row(
            Region_Desc="West",
            Banner="Thrifty Foods",
            Section_Master="Frozen Breakfast",
            Percentage_Space="0.15",
        ),
        Row(
            Region_Desc="Quebec",
            Banner="IGA",
            Section_Master="Frozen Breakfast",
            Percentage_Space="0.2",
        ),
        Row(
            Region_Desc="West",
            Banner="IGA",
            Section_Master="Frozen Breakfast",
            Percentage_Space="0.2",
        ),
    ]
)
