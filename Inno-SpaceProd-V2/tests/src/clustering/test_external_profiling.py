"""
How mock data was generated for the 'test_region_banner_profiling'


def create_mock_data_for_ext_profiling(
    exp: str,
    df_ext_clust_prof: SparkDataFrame,
    df_ext_clust_output: SparkDataFrame,
    list_mock_stores: List[str],
):

    # these are the GROUPS of columns we need, we only need 1 out
    list_suff = [
        f"{exp}_Household_Income_",
        "Cmp_Segment",
        "Cmp_Ban_Type",
        f"{exp}_Total_Visible_Minorities_",
        f"{exp}_Total_Population_10",
        f"{exp}_Total_Population_0_To_4",
        f"{exp}_Household_Population_15_Years_Or_Over_For_Educational_Attainment_",
    ]

    cols_act_prof = df_ext_clust_prof.columns
    cols_act_output = df_ext_clust_output.columns

    cols_other_prof = [
        "Banner_Key",
        "Index",
        "Tot_Wkly_Sales",
        "Tot_Sales_Area",
        "Tot_Gross_Area",
        "Store_Physical_Location_No",
        "Region_Desc",
        "Banner",
        "Cluster_Labels",
    ]

    cols_other_output = [
        'Region_Desc', 'Banner','Index', 'Store_Physical_Location_No', 'Cluster_Labels',
    ]

    # we take only the 1st column in each group
    cols_suff_prof = [([x for x in cols_act_prof if (x.startswith(z) and x.endswith("_10"))][0]) for z in list_suff]

    # same with this one, but for this one it must end with _10
    cols_suff_output = [[x for x in cols_act_output if (x.startswith(z) and x.endswith("_10"))][0] or None for z in list_suff]

    assert len(cols_suff_prof)>0, "empty cols_suff_prof"
    assert len(cols_suff_output)>0, "empty cols_suff_output"

    msg= "no valid cols for cols_suff_prof"
    assert all([x is not None for x in cols_suff_prof]), msg
    msg = "no valid cols for cols_suff_output"
    assert all([x is not None for x in cols_suff_output]), msg

    cols_mock_prof = cols_other_prof + cols_suff_prof
    cols_mock_output = cols_other_output + cols_suff_output

    df_ext_clust_prof_mock = df_ext_clust_prof.select(*cols_mock_prof)
    df_ext_clust_output_mock= df_ext_clust_output.select(*cols_mock_output)

    mask = F.col("Store_Physical_Location_No").cast(T.IntegerType()).cast(T.StringType()).isin(list_mock_stores)
    df_ext_clust_prof_mock = df_ext_clust_prof_mock.filter(mask)
    df_ext_clust_output_mock = df_ext_clust_output_mock.filter(mask)

    assert df_ext_clust_prof_mock.count()>0, "empty df_ext_clust_prof_mock"
    assert df_ext_clust_output_mock.count()>0, "empty df_ext_clust_output_mock"

    return df_ext_clust_prof_mock, df_ext_clust_output_mock

run="dbfs:/mnt/blob/sobeys_space_prod/test_space_run_20220223_233039_390177"

context.reload_context_from_run_folder(run)

df_ext_clust_prof_mock, df_ext_clust_output_mock = create_mock_data_for_ext_profiling(
    exp=exp,
    df_ext_clust_prof=context.data.read("final_profiling_data"),
    df_ext_clust_output=context.data.read("final_clustering_data"),
    list_mock_stores=["13050", "23465"],
)

"""
import pytest
from tests.utils.spark_util import spark_tests

from pyspark import Row
from spaceprod.src.clustering.external_profiling.helpers import (
    exclude_invalid_region_banners,
    get_common_region_banner,
    region_banner_profiling,
)


@pytest.fixture(scope="session")
def df_ext_clust_prof_mock():
    df_ext_clust_prof_mock = spark_tests.sparkContext.parallelize(
        [
            Row(
                "1020641",
                "78",
                "173063",
                "16000",
                "22000",
                "23465",
                "Ontario",
                "SOBEYS",
                "2",
                "2707",
                "10.0",
                "1.0",
                "54661",
                "190627",
                "9359",
                "161487",
            ),
            Row(
                "1015709",
                "62",
                "493371",
                "29865",
                "41338",
                "13050",
                "Ontario",
                "SOBEYS",
                "1",
                "2063",
                "10.0",
                "0.0",
                "11522",
                "90074",
                "3914",
                "75685",
            ),
        ]
    ).toDF(
        [
            "Banner_Key",
            "Index",
            "Tot_Wkly_Sales",
            "Tot_Sales_Area",
            "Tot_Gross_Area",
            "Store_Physical_Location_No",
            "Region_Desc",
            "Banner",
            "Cluster_Labels",
            "2021_Household_Income_0_To_19999_Current_Year__10",
            "Cmp_Segment_Discount_10",
            "Cmp_Ban_Type_Costco_Canada_Discount_10",
            "2021_Total_Visible_Minorities_10",
            "2021_Total_Population_10",
            "2021_Total_Population_0_To_4_10",
            "2021_Household_Population_15_Years_Or_Over_For_Educational_Attainment_10",
        ]
    )

    return df_ext_clust_prof_mock


@pytest.fixture(scope="session")
def df_ext_clust_output_mock():
    df_ext_clust_output_mock = spark_tests.sparkContext.parallelize(
        [
            Row(
                "Ontario",
                "SOBEYS",
                "78.0",
                "23465.0",
                "2",
                "0.03401181101799011",
                "0.0",
                "0.0",
                "0.29129695892333984",
                "190627.0",
                "0.04909587651491165",
                "161487.0",
            ),
            Row(
                "Ontario",
                "SOBEYS",
                "62.0",
                "13050.0",
                "2",
                "0.0521078035235405",
                "1.0",
                "0.0",
                "0.13155370950698853",
                "90074.0",
                "0.0434531606733799",
                "75685.0",
            ),
        ]
    ).toDF(
        [
            "Region_Desc",
            "Banner",
            "Index",
            "Store_Physical_Location_No",
            "Cluster_Labels",
            "2021_Household_Income_0_To_19999_Current_Year__10",
            "Cmp_Segment_0_10",
            "Cmp_Ban_Type_Amazon_Inc_Full_Service_10",
            "2021_Total_Visible_Minorities_10",
            "2021_Total_Population_10",
            "2021_Total_Population_0_To_4_10",
            "2021_Household_Population_15_Years_Or_Over_For_Educational_Attainment_10",
        ]
    )

    return df_ext_clust_output_mock


def test_region_banner_profiling(df_ext_clust_prof_mock, df_ext_clust_output_mock):

    permutations_mock = [{"Region_Desc": "Ontario", "Banner": "SOBEYS"}]

    africa_grp = [
        "Africa",
        "Tanzania",
        "Somalia",
        "Tunisia",
        "Cte_Divoire",
        "Eritrea",
        "Ethiopia",
        "Kenya",
        "Morocco",
        "Lebanon",
        "Eritrea",
        "Ghana",
        "Cameroon",
        "Algeria",
    ]
    asia_grp = [
        "Asia",
        "Taiwan",
        "Japan",
        "Hong_Kong",
        "Sri_Lanka",
        "United_Arab_Emirates",
        "Syria",
        "India",
        "Afghanistan",
        "China",
        "Pakistan",
        "Saudi_Arbia",
        "Nepal",
        "Malaysia",
        "Philippines",
        "Cambodia",
        "Vietnam",
        "Iraq",
        "Bangladesh",
    ]
    europe_grp = [
        "Europe",
        "Italy",
        "Turkey",
        "Bosnia_Herzegovina",
        "Ukraine",
        "Romania",
        "Netherlands",
        "Germany",
        "Czech_Republic",
        "France",
        "Greece",
        "Portugal",
        "Hungary",
        "Poland",
        "Croatia",
        "Ireland",
        "Serbia",
    ]
    latin_america_grp = [
        "South_America",
        "Caribbean_And_Bermuda",
        "Colombia",
        "Chile",
        "Brazil",
        "Mexico",
        "Venezuela",
        "Haiti",
        "Cuba",
        "Peru",
        "El_Salvador",
    ]
    feature_list = [
        "africa_immigration",
        "asia_immigration",
        "europe_immigration",
        "latin_america_immigration",
        "minorities",
        "population_age",
        "household_income",
        "other_demographics",
        "education",
        "competitor_segment_density",
        "competitor_banner_density",
        "other_competitors",
    ]
    exp = "2021"

    df_result = region_banner_profiling(
        permutations=permutations_mock,
        df_ext_clust_prof=df_ext_clust_prof_mock,
        df_ext_clust_output=df_ext_clust_output_mock,
        africa_grp=africa_grp,
        asia_grp=asia_grp,
        europe_grp=europe_grp,
        latin_america_grp=latin_america_grp,
        feature_list=feature_list,
        exp=exp,
    )

    cols_exp = [
        "Cluster_Labels",
        "var",
        "var_details",
        "Index",
        "Index_Stdev",
        "REGION",
        "BANNER",
    ]
    cols_act = df_result.columns
    assert set(cols_exp) == set(cols_act), "wrong cols"
    assert len(df_result) == 12


def test_exclude_invalid_region_banners():
    df_profiling_mock = spark_tests.createDataFrame(
        [
            Row(Index=None, Index_Stdev=None, REGION="Ontario", BANNER="SOBEYS"),
            Row(Index="100.0", Index_Stdev="100.0", REGION="Ontario", BANNER="SOBEYS"),
        ]
    )

    pdf_profiling = df_profiling_mock.toPandas()

    # one of the rows should be filtered out
    df_output = exclude_invalid_region_banners(pdf_profiling=pdf_profiling)

    assert df_output.count() == 1


def test_get_common_region_banner(df_ext_clust_output_mock, df_ext_clust_prof_mock):
    permutations = get_common_region_banner(
        df_ext_clust_output=df_ext_clust_output_mock,
        df_ext_clust_prof=df_ext_clust_prof_mock,
    )

    permutations_expected = [{"Region_Desc": "Ontario", "Banner": "SOBEYS"}]
    assert permutations == permutations_expected
