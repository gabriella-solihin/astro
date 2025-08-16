import pandas as pd
import pytest

from pyspark import Row
from pyspark.sql import SparkSession
from spaceprod.src.clustering.need_states_creation.pre_processing.helpers import (
    add_customer_card_id,
    clean_section_master,
    clean_spaceman,
    combine_pogs_into_section_masters,
    dedup_apollo,
    dedup_spaceman,
    filter_apollo,
    get_consistent_item_widths,
    get_core_trans,
    get_frequent_visitors,
    get_items_diff_trans,
    get_items_lost_after_explosion,
    get_section_master_modes,
    merge_pogs,
    patch_pog,
    remove_low_volume_items,
    translate_spaceman,
    udf_impute_by_facings,
    validate_using_exec_id,
    add_case_pack_info,
)
from spaceprod.utils.data_helpers import backup_on_blob
from spaceprod.utils.data_transformation import check_invalid
from spaceprod.utils.imports import F, SparkDataFrame


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    from tests.utils.spark_util import spark_tests as spark

    return spark


@pytest.fixture(scope="session")
def df_apollo_validated(spark):
    df_apollo_validated = spark.createDataFrame(
        [
            Row(
                STATUS="Released",
                RELEASE_DATE="01APR2021",
                STORE="1",
                STOCKCODE="111",
                SECTION_NAME="name a 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER=1,
                TERMINATION_DATE="01MAR2021",
                POSITION=1,
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="02APR2021",
                STORE="1",
                STOCKCODE="111",
                SECTION_NAME="name b 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER=1,
                TERMINATION_DATE="02MAR2021",
                POSITION=1,
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="03APR2021",
                STORE="1",
                STOCKCODE="111",
                SECTION_NAME="name c 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER=1,
                TERMINATION_DATE="03MAR2021",
                POSITION=1,
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="04APR2021",
                STORE="2",
                STOCKCODE="222",
                SECTION_NAME="name d 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER=1,
                TERMINATION_DATE="04MAR2021",
                POSITION=1,
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="05APR2021",
                STORE="2",
                STOCKCODE="222",
                SECTION_NAME="name e 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER=1,
                TERMINATION_DATE="05MAR2021",
                POSITION=1,
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="06APR2021",
                STORE="2",
                STOCKCODE="222",
                SECTION_NAME="name f 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER=1,
                TERMINATION_DATE="06MAR2021",
                POSITION=1,
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="07APR2021",
                STORE="3",
                STOCKCODE="333",
                SECTION_NAME="name g 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER=1,
                TERMINATION_DATE="07MAR2021",
                POSITION=1,
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="08APR2021",
                STORE="3",
                STOCKCODE="333",
                SECTION_NAME="name h 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER=1,
                TERMINATION_DATE="08MAR2021",
                POSITION=1,
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="09APR2021",
                STORE="3",
                STOCKCODE="333",
                SECTION_NAME="name i 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER=1,
                TERMINATION_DATE="09MAR2021",
                POSITION=1,
            ),
            Row(
                STATUS="Bad     ",
                RELEASE_DATE="10APR2021",
                STORE="4",
                STOCKCODE="444",
                SECTION_NAME="name j 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER=1,
                TERMINATION_DATE="10MAR2021",
                POSITION=1,
            ),
        ]
    )

    return df_apollo_validated


@pytest.fixture(scope="session")
def df_apollo_deduped(spark):
    df_apollo_deduped = spark.createDataFrame(
        [
            Row(
                STORE="1",
                SECTION_MASTER="Namea",
                SECTION_NAME="name a 3FT (123)",
                STATUS="Released",
                RELEASE_DATE="01APR2021",
                ITEM_NO="111",
                RETAILER="1",
                SHELF_NUMBER="1",
                TERMINATION_DATE="01MAR2021",
                POSITION="1",
            ),
            Row(
                STORE="2",
                SECTION_MASTER="Named",
                SECTION_NAME="name d 3FT (123)",
                STATUS="Released",
                RELEASE_DATE="04APR2021",
                ITEM_NO="222",
                RETAILER="1",
                SHELF_NUMBER="1",
                TERMINATION_DATE="04MAR2021",
                POSITION="1",
            ),
            Row(
                STORE="3",
                SECTION_MASTER="Nameg",
                SECTION_NAME="name g 3FT (123)",
                STATUS="Released",
                RELEASE_DATE="07APR2021",
                ITEM_NO="333",
                RETAILER="1",
                SHELF_NUMBER="1",
                TERMINATION_DATE="07MAR2021",
                POSITION="1",
            ),
        ]
    )

    return df_apollo_deduped


@pytest.fixture(scope="session")
def df_apollo_sm(spark):

    df_apollo_sm = spark.createDataFrame(
        [
            Row(
                STATUS="Released",
                RELEASE_DATE="01APR2021",
                STORE="1",
                ITEM_NO="111",
                SECTION_NAME="name a 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER="1",
                TERMINATION_DATE="01MAR2021",
                POSITION="1",
                SECTION_MASTER="Namea",
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="02APR2021",
                STORE="1",
                ITEM_NO="111",
                SECTION_NAME="name b 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER="1",
                TERMINATION_DATE="02MAR2021",
                POSITION="1",
                SECTION_MASTER="Nameb",
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="03APR2021",
                STORE="1",
                ITEM_NO="111",
                SECTION_NAME="name c 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER="1",
                TERMINATION_DATE="03MAR2021",
                POSITION="1",
                SECTION_MASTER="Namec",
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="04APR2021",
                STORE="2",
                ITEM_NO="222",
                SECTION_NAME="name d 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER="1",
                TERMINATION_DATE="04MAR2021",
                POSITION="1",
                SECTION_MASTER="Named",
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="05APR2021",
                STORE="2",
                ITEM_NO="222",
                SECTION_NAME="name e 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER="1",
                TERMINATION_DATE="05MAR2021",
                POSITION="1",
                SECTION_MASTER="Namee",
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="06APR2021",
                STORE="2",
                ITEM_NO="222",
                SECTION_NAME="name f 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER="1",
                TERMINATION_DATE="06MAR2021",
                POSITION="1",
                SECTION_MASTER="Namef",
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="07APR2021",
                STORE="3",
                ITEM_NO="333",
                SECTION_NAME="name g 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER="1",
                TERMINATION_DATE="07MAR2021",
                POSITION="1",
                SECTION_MASTER="Nameg",
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="08APR2021",
                STORE="3",
                ITEM_NO="333",
                SECTION_NAME="name h 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER="1",
                TERMINATION_DATE="08MAR2021",
                POSITION="1",
                SECTION_MASTER="Nameh",
            ),
            Row(
                STATUS="Released",
                RELEASE_DATE="09APR2021",
                STORE="3",
                ITEM_NO="333",
                SECTION_NAME="name i 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER="1",
                TERMINATION_DATE="09MAR2021",
                POSITION="1",
                SECTION_MASTER="Namei",
            ),
            Row(
                STATUS="Bad     ",
                RELEASE_DATE="10APR2021",
                STORE="4",
                ITEM_NO="444",
                SECTION_NAME="name j 3FT (123)",
                RETAILER="1",
                SHELF_NUMBER="1",
                TERMINATION_DATE="10MAR2021",
                POSITION="1",
                SECTION_MASTER="Namej",
            ),
        ]
    )

    return df_apollo_sm


@pytest.fixture(scope="session")
def df_spaceman_validated(spark):
    df_spaceman_validated = spark.createDataFrame(
        [
            Row(
                STORE="1",
                STOCKCODE="111             ",
                RELEASE_DATE="2021-10-01 07:58:38.650",
                SECTION_NAME="name c 3FT (123)",
            ),
            Row(
                STORE="1",
                STOCKCODE="111             ",
                RELEASE_DATE="2021-10-02 07:58:38.650",
                SECTION_NAME="name c 3FT (123)",
            ),
            Row(
                STORE="1",
                STOCKCODE="111             ",
                RELEASE_DATE="2021-10-03 08:34:10.440",
                SECTION_NAME="name c 3FT (123)",
            ),
            Row(
                STORE="2",
                STOCKCODE="222             ",
                RELEASE_DATE="2021-10-04 08:34:10.440",
                SECTION_NAME="name f 3FT (123)",
            ),
            Row(
                STORE="2",
                STOCKCODE="222             ",
                RELEASE_DATE="2021-10-05 08:34:10.440",
                SECTION_NAME="name f 3FT (123)",
            ),
            Row(
                STORE="2",
                STOCKCODE="222             ",
                RELEASE_DATE="2021-10-06 08:34:10.440",
                SECTION_NAME="name f 3FT (123)",
            ),
            Row(
                STORE="3",
                STOCKCODE="333             ",
                RELEASE_DATE="2021-10-07 08:34:10.440",
                SECTION_NAME="name i 3FT (123)",
            ),
            Row(
                STORE="3",
                STOCKCODE="333             ",
                RELEASE_DATE="2021-10-08 08:34:10.440",
                SECTION_NAME="name i 3FT (123)",
            ),
            Row(
                STORE="3",
                STOCKCODE="333             ",
                RELEASE_DATE="2021-10-09 08:34:10.440",
                SECTION_NAME="name i 3FT (123)",
            ),
            Row(
                STORE="3",
                STOCKCODE="333             ",
                RELEASE_DATE="2021-10-10 08:34:10.440",
                SECTION_NAME="name zz 3FT (123)",
            ),
        ]
    )

    return df_spaceman_validated


@pytest.fixture(scope="session")
def df_translations(spark):
    df_translations = spark.createDataFrame(
        [
            Row(
                SobeysPlanoKey_Planogram_FR="name zz 3FT (123)",
                SobeysPlanoKey_Planogram_EN="name zz 3FT (123) translated",
            ),
        ]
    )

    return df_translations


@pytest.fixture(scope="session")
def df_spaceman_deduped(spark):
    df_spaceman_deduped = spark.createDataFrame(
        [
            Row(
                STORE="1",
                FR_SECTION_MASTER="",
                SECTION_NAME="name c 3FT (123)",
                RELEASE_DATE="03Oct2021",
                ITEM_NO="111",
            ),
            Row(
                STORE="2",
                FR_SECTION_MASTER="",
                SECTION_NAME="name f 3FT (123)",
                RELEASE_DATE="06Oct2021",
                ITEM_NO="222",
            ),
            Row(
                STORE="3",
                FR_SECTION_MASTER="",
                SECTION_NAME="name zz 3FT (123)",
                RELEASE_DATE="10Oct2021",
                ITEM_NO="333",
            ),
        ]
    )

    return df_spaceman_deduped


@pytest.fixture(scope="session")
def df_spaceman_cleaned(spark):
    df_spaceman_cleaned = spark.createDataFrame(
        [
            Row(
                STORE="1",
                ITEM_NO="111",
                RELEASE_DATE="01Oct2021",
                SECTION_NAME="name c 3FT (123)",
                FR_SECTION_MASTER="",
            ),
            Row(
                STORE="1",
                ITEM_NO="111",
                RELEASE_DATE="02Oct2021",
                SECTION_NAME="name c 3FT (123)",
                FR_SECTION_MASTER="",
            ),
            Row(
                STORE="1",
                ITEM_NO="111",
                RELEASE_DATE="03Oct2021",
                SECTION_NAME="name c 3FT (123)",
                FR_SECTION_MASTER="",
            ),
            Row(
                STORE="2",
                ITEM_NO="222",
                RELEASE_DATE="04Oct2021",
                SECTION_NAME="name f 3FT (123)",
                FR_SECTION_MASTER="",
            ),
            Row(
                STORE="2",
                ITEM_NO="222",
                RELEASE_DATE="05Oct2021",
                SECTION_NAME="name f 3FT (123)",
                FR_SECTION_MASTER="",
            ),
            Row(
                STORE="2",
                ITEM_NO="222",
                RELEASE_DATE="06Oct2021",
                SECTION_NAME="name f 3FT (123)",
                FR_SECTION_MASTER="",
            ),
            Row(
                STORE="3",
                ITEM_NO="333",
                RELEASE_DATE="07Oct2021",
                SECTION_NAME="name i 3FT (123)",
                FR_SECTION_MASTER="",
            ),
            Row(
                STORE="3",
                ITEM_NO="333",
                RELEASE_DATE="08Oct2021",
                SECTION_NAME="name i 3FT (123)",
                FR_SECTION_MASTER="",
            ),
            Row(
                STORE="3",
                ITEM_NO="333",
                RELEASE_DATE="09Oct2021",
                SECTION_NAME="name i 3FT (123)",
                FR_SECTION_MASTER="",
            ),
            Row(
                STORE="3",
                ITEM_NO="333",
                RELEASE_DATE="10Oct2021",
                SECTION_NAME="name zz 3FT (123)",
                FR_SECTION_MASTER="",
            ),
        ]
    )

    return df_spaceman_cleaned


@pytest.fixture(scope="session")
def df_spaceman_sm(spark):
    df_spaceman_sm = spark.createDataFrame(
        [
            Row(
                SECTION_NAME="name c 3FT (123)",
                STORE="1",
                RELEASE_DATE="03Oct2021",
                ITEM_NO="111",
                SECTION_MASTER="Namec",
            ),
            Row(
                SECTION_NAME="name f 3FT (123)",
                STORE="2",
                RELEASE_DATE="06Oct2021",
                ITEM_NO="222",
                SECTION_MASTER="Namef",
            ),
            Row(
                SECTION_NAME="name zz 3FT (123)",
                STORE="3",
                RELEASE_DATE="10Oct2021",
                ITEM_NO="333",
                SECTION_MASTER="Namei",
            ),
        ]
    )

    return df_spaceman_sm


def test_clean_section_master(spark, df_apollo_sm, df_apollo_validated):

    df_apollo_sm_actual = clean_section_master(
        df_apollo=df_apollo_validated,
    )

    df_check = df_apollo_sm_actual.exceptAll(df_apollo_sm)
    assert df_check.limit(1).count() == 0


def test_dedup_apollo(
    spark,
    df_apollo_sm: SparkDataFrame,
    df_apollo_deduped: SparkDataFrame,
):
    df_apollo_deduped_actual = dedup_apollo(df_apollo_sm)
    df_apollo_deduped_actual = filter_apollo(df_apollo_deduped_actual)
    df_check = df_apollo_deduped_actual.exceptAll(df_apollo_deduped)
    assert df_check.limit(1).count() == 0


def test_clean_spaceman(df_spaceman_validated, df_spaceman_cleaned):
    df_spaceman_cleaned_actual = clean_spaceman(df_spaceman_validated)
    df_check = df_spaceman_cleaned_actual.exceptAll(df_spaceman_cleaned)
    assert df_check.limit(1).count() == 0


def test_dedup_spaceman(df_spaceman_cleaned, df_spaceman_deduped):
    df_spaceman_deduped_actual = dedup_spaceman(df_spaceman_cleaned)
    df_check = df_spaceman_deduped_actual.exceptAll(df_spaceman_deduped)
    assert df_check.limit(1).count() == 0


def test_post_pogs_processing(
    spark: SparkSession,
    df_apollo_sm: SparkDataFrame,
    df_spaceman_sm: SparkDataFrame,
):

    # patch one of the datsets to make sure store numbers are different
    col = F.concat(F.col("STORE"), F.lit("0"))
    df_a_sm = df_apollo_sm.withColumn("STORE", col)

    # add additional less relevant columns with mock data
    cols_dummy = ["FACINGS", "WIDTH", "HEIGHT", "DEPTH", "LENGTH_SECTION_INCHES"]
    cols = [F.col("*")] + [F.lit(1).alias(x) for x in cols_dummy]
    df_a_sm = df_a_sm.select(*cols)
    df_s_sm = df_spaceman_sm.select(*cols)

    df_pogs_merged = merge_pogs(
        df_apollo=df_a_sm,
        df_spaceman=df_s_sm,
    )

    assert df_a_sm.count() + df_s_sm.count() == df_pogs_merged.count()

    df_expected = spark.createDataFrame(
        [
            Row(
                STORE="10",
                ITEM_NO="111",
                SECTION_NAME="name a 3FT (123)",
                SECTION_MASTER="Namea",
                FACINGS=1,
                WIDTH=1.0,
                HEIGHT=1.0,
                DEPTH=1.0,
                LENGTH_SECTION_INCHES=1,
                RELEASE_DATE="01APR2021",
                SOURCE="APOLLO",
            ),
            Row(
                STORE="10",
                ITEM_NO="111",
                SECTION_NAME="name b 3FT (123)",
                SECTION_MASTER="Nameb",
                FACINGS=1,
                WIDTH=1.0,
                HEIGHT=1.0,
                DEPTH=1.0,
                LENGTH_SECTION_INCHES=1,
                RELEASE_DATE="02APR2021",
                SOURCE="APOLLO",
            ),
            Row(
                STORE="10",
                ITEM_NO="111",
                SECTION_NAME="name c 3FT (123)",
                SECTION_MASTER="Namec",
                FACINGS=1,
                WIDTH=1.0,
                HEIGHT=1.0,
                DEPTH=1.0,
                LENGTH_SECTION_INCHES=1,
                RELEASE_DATE="03APR2021",
                SOURCE="APOLLO",
            ),
            Row(
                STORE="20",
                ITEM_NO="222",
                SECTION_NAME="name d 3FT (123)",
                SECTION_MASTER="Named",
                FACINGS=1,
                WIDTH=1.0,
                HEIGHT=1.0,
                DEPTH=1.0,
                LENGTH_SECTION_INCHES=1,
                RELEASE_DATE="04APR2021",
                SOURCE="APOLLO",
            ),
            Row(
                STORE="20",
                ITEM_NO="222",
                SECTION_NAME="name e 3FT (123)",
                SECTION_MASTER="Namee",
                FACINGS=1,
                WIDTH=1.0,
                HEIGHT=1.0,
                DEPTH=1.0,
                LENGTH_SECTION_INCHES=1,
                RELEASE_DATE="05APR2021",
                SOURCE="APOLLO",
            ),
            Row(
                STORE="20",
                ITEM_NO="222",
                SECTION_NAME="name f 3FT (123)",
                SECTION_MASTER="Namef",
                FACINGS=1,
                WIDTH=1.0,
                HEIGHT=1.0,
                DEPTH=1.0,
                LENGTH_SECTION_INCHES=1,
                RELEASE_DATE="06APR2021",
                SOURCE="APOLLO",
            ),
            Row(
                STORE="30",
                ITEM_NO="333",
                SECTION_NAME="name g 3FT (123)",
                SECTION_MASTER="Nameg",
                FACINGS=1,
                WIDTH=1.0,
                HEIGHT=1.0,
                DEPTH=1.0,
                LENGTH_SECTION_INCHES=1,
                RELEASE_DATE="07APR2021",
                SOURCE="APOLLO",
            ),
            Row(
                STORE="30",
                ITEM_NO="333",
                SECTION_NAME="name h 3FT (123)",
                SECTION_MASTER="Nameh",
                FACINGS=1,
                WIDTH=1.0,
                HEIGHT=1.0,
                DEPTH=1.0,
                LENGTH_SECTION_INCHES=1,
                RELEASE_DATE="08APR2021",
                SOURCE="APOLLO",
            ),
            Row(
                STORE="30",
                ITEM_NO="333",
                SECTION_NAME="name i 3FT (123)",
                SECTION_MASTER="Namei",
                FACINGS=1,
                WIDTH=1.0,
                HEIGHT=1.0,
                DEPTH=1.0,
                LENGTH_SECTION_INCHES=1,
                RELEASE_DATE="09APR2021",
                SOURCE="APOLLO",
            ),
            Row(
                STORE="40",
                ITEM_NO="444",
                SECTION_NAME="name j 3FT (123)",
                SECTION_MASTER="Namej",
                FACINGS=1,
                WIDTH=1.0,
                HEIGHT=1.0,
                DEPTH=1.0,
                LENGTH_SECTION_INCHES=1,
                RELEASE_DATE="10APR2021",
                SOURCE="APOLLO",
            ),
            Row(
                STORE="1",
                ITEM_NO="111",
                SECTION_NAME="name c 3FT (123)",
                SECTION_MASTER="Namec",
                FACINGS=1,
                WIDTH=1.0,
                HEIGHT=1.0,
                DEPTH=1.0,
                LENGTH_SECTION_INCHES=1,
                RELEASE_DATE="03Oct2021",
                SOURCE="SPACEMAN",
            ),
            Row(
                STORE="2",
                ITEM_NO="222",
                SECTION_NAME="name f 3FT (123)",
                SECTION_MASTER="Namef",
                FACINGS=1,
                WIDTH=1.0,
                HEIGHT=1.0,
                DEPTH=1.0,
                LENGTH_SECTION_INCHES=1,
                RELEASE_DATE="06Oct2021",
                SOURCE="SPACEMAN",
            ),
            Row(
                STORE="3",
                ITEM_NO="333",
                SECTION_NAME="name zz 3FT (123)",
                SECTION_MASTER="Namei",
                FACINGS=1,
                WIDTH=1.0,
                HEIGHT=1.0,
                DEPTH=1.0,
                LENGTH_SECTION_INCHES=1,
                RELEASE_DATE="10Oct2021",
                SOURCE="SPACEMAN",
            ),
        ]
    )

    df_pogs_merged = backup_on_blob(spark, df_pogs_merged)

    df_check = df_pogs_merged.exceptAll(df_expected)
    assert df_check.limit(1).count() == 0

    df_sm_override = spark.createDataFrame(
        [
            Row(
                SECTION_MASTER="Namea",
                UPDATED_SECTION_MASTER="patched_a",
                Region="R1",
                Banner="B1",
            ),
            Row(
                SECTION_MASTER="Nameb",
                UPDATED_SECTION_MASTER="patched_b",
                Region="R1",
                Banner="B1",
            ),
            Row(
                SECTION_MASTER="Namec",
                UPDATED_SECTION_MASTER="patched_c",
                Region="R1",
                Banner="B1",
            ),
            Row(
                SECTION_MASTER="Named",
                UPDATED_SECTION_MASTER="patched_d",
                Region="R1",
                Banner="B1",
            ),
            Row(
                SECTION_MASTER="Namee",
                UPDATED_SECTION_MASTER="patched_e",
                Region="R1",
                Banner="B1",
            ),
            Row(
                SECTION_MASTER="Namef",
                UPDATED_SECTION_MASTER="patched_f",
                Region="R1",
                Banner="B1",
            ),
            Row(
                SECTION_MASTER="Nameg",
                UPDATED_SECTION_MASTER="patched_g",
                Region="R1",
                Banner="B1",
            ),
            Row(
                SECTION_MASTER="Nameh",
                UPDATED_SECTION_MASTER="patched_h",
                Region="R1",
                Banner="B1",
            ),
            Row(
                SECTION_MASTER="Namei",
                UPDATED_SECTION_MASTER="patched_i",
                Region="R1",
                Banner="B1",
            ),
            Row(
                SECTION_MASTER="Namej",
                UPDATED_SECTION_MASTER="patched_j",
                Region="R1",
                Banner="B1",
            ),
        ]
    )

    # dummy location data
    df_location = spark.createDataFrame(
        [
            Row(
                REGION_DESC="R1",
                NATIONAL_BANNER_DESC="B1",
                STORE_NO=10,
                RETAIL_OUTLET_LOCATION_SK=101,
                ACTIVE_STATUS_CD="A",
            ),
            Row(
                REGION_DESC="R1",
                NATIONAL_BANNER_DESC="B1",
                STORE_NO=40,
                RETAIL_OUTLET_LOCATION_SK=401,
                ACTIVE_STATUS_CD="A",
            ),
            Row(
                REGION_DESC="R1",
                NATIONAL_BANNER_DESC="B1",
                STORE_NO=20,
                RETAIL_OUTLET_LOCATION_SK=201,
                ACTIVE_STATUS_CD="A",
            ),
            Row(
                REGION_DESC="R1",
                NATIONAL_BANNER_DESC="B1",
                STORE_NO=30,
                RETAIL_OUTLET_LOCATION_SK=301,
                ACTIVE_STATUS_CD="A",
            ),
            Row(
                REGION_DESC="R1",
                NATIONAL_BANNER_DESC="B1",
                STORE_NO=2,
                RETAIL_OUTLET_LOCATION_SK=21,
                ACTIVE_STATUS_CD="A",
            ),
            Row(
                REGION_DESC="R1",
                NATIONAL_BANNER_DESC="B1",
                STORE_NO=1,
                RETAIL_OUTLET_LOCATION_SK=11,
                ACTIVE_STATUS_CD="A",
            ),
            Row(
                REGION_DESC="R1",
                NATIONAL_BANNER_DESC="B1",
                STORE_NO=3,
                RETAIL_OUTLET_LOCATION_SK=31,
                ACTIVE_STATUS_CD="A",
            ),
        ]
    )

    # testing patch_pog
    df_pogs_patched = patch_pog(
        df_pog=df_pogs_merged,
        df_sm_override=df_sm_override,
        df_location=df_location,
        thresh_ratio_missing_pogs=0.15,
        thresh_ratio_missing_store_pogs=0.15,
    )

    vals_expected = [
        "patched_a",
        "patched_g",
        "patched_e",
        "patched_j",
        "patched_d",
        "patched_f",
        "patched_c",
        "patched_h",
        "patched_i",
        "patched_b",
    ]

    df_check = df_pogs_patched.filter(~F.col("SECTION_MASTER").isin(vals_expected))
    assert df_check.limit(1).count() == 0

    # try an empty override table - should break
    df_sm_override = df_sm_override.filter(F.lit(1) == 2)

    broke = False

    try:
        patch_pog(
            df_pog=df_pogs_merged,
            df_sm_override=df_sm_override,
            df_location=df_location,
            thresh_ratio_missing_pogs=0.15,
            thresh_ratio_missing_store_pogs=0.15,
        )
    except Exception:
        broke = True

    assert broke, "must break with empty (outdated) override table"

    # testing get_consistent_item_widths
    df_pogs_width = get_consistent_item_widths(df_pogs_merged)
    df_check = df_pogs_width.filter(F.col("WIDTH") != 1)
    assert df_check.limit(1).count() == 0

    # try with a different width for item 222 in store 20
    mask = (F.col("STORE") == 20) & (F.col("ITEM_NO") == 222)
    col = F.when(mask, F.lit(2)).otherwise(F.lit(1))
    df_pogs_width = get_consistent_item_widths(df_pogs_merged.withColumn("WIDTH", col))
    df_check = df_pogs_width.filter(F.col("WIDTH") != 1)
    assert df_check.count() == 4

    # dummy location data
    df_location = spark.createDataFrame(
        [
            Row(
                REGION_DESC="region",
                NATIONAL_BANNER_DESC="ban",
                STORE_NO=10,
                RETAIL_OUTLET_LOCATION_SK=1,
                ACTIVE_STATUS_CD="A",
            )
        ]
    )

    # testing get_section_master_modes
    df_pogs_modes = get_section_master_modes(
        df_pog=df_pogs_merged,
        df_location=df_location,
    )

    assert df_pogs_modes.count() == 1


def test_get_core_trans_need_states(spark: SparkSession):

    df_trans = spark.createDataFrame(
        [
            Row(
                ITEM_SK="1",
                RETAIL_OUTLET_LOCATION_SK="10",
                CALENDAR_DT="2020-01-01",
                CUSTOMER_SK="1",
                CUSTOMER_CARD_SK="1",
                TRANSACTION_RK="111",
                REGION="ontario",
            ),
            Row(
                ITEM_SK="1",
                RETAIL_OUTLET_LOCATION_SK="20",
                CALENDAR_DT="2020-01-01",
                CUSTOMER_SK="2",
                CUSTOMER_CARD_SK="2",
                TRANSACTION_RK="222",
                REGION="ontario",
            ),
            Row(
                ITEM_SK="2",
                RETAIL_OUTLET_LOCATION_SK="10",
                CALENDAR_DT="2020-01-01",
                CUSTOMER_SK="3",
                CUSTOMER_CARD_SK="3",
                TRANSACTION_RK="333",
                REGION="ontario",
            ),
            Row(
                ITEM_SK="2",
                RETAIL_OUTLET_LOCATION_SK="20",
                CALENDAR_DT="2020-01-01",
                CUSTOMER_SK="4",
                CUSTOMER_CARD_SK="4",
                TRANSACTION_RK="444",
                REGION="ontario",
            ),
            Row(
                ITEM_SK="5",
                RETAIL_OUTLET_LOCATION_SK="30",
                CALENDAR_DT="2020-01-01",
                CUSTOMER_SK="5",
                CUSTOMER_CARD_SK="5",
                TRANSACTION_RK="555",
                REGION="ontario",
            ),
        ]
    )

    df_banner_lookup = spark.createDataFrame(
        [
            Row(
                STORE_NO="100",
                RETAIL_OUTLET_LOCATION_SK="10",
                NATIONAL_BANNER_DESC="Banner 10",
            ),
            Row(
                STORE_NO="200",
                RETAIL_OUTLET_LOCATION_SK="20",
                NATIONAL_BANNER_DESC="Banner 20",
            ),
            Row(
                STORE_NO="300",
                RETAIL_OUTLET_LOCATION_SK="30",
                NATIONAL_BANNER_DESC="Banner 30",
            ),
        ]
    )

    df_prod_hierarchy = spark.createDataFrame(
        [
            Row(ITEM_NO="101", ITEM_SK="1"),
            Row(ITEM_NO="202", ITEM_SK="2"),
            Row(ITEM_NO="303", ITEM_SK="3"),
        ]
    )

    df_item_pog_section_lookup = spark.createDataFrame(
        [
            Row(SECTION_MASTER="SM 101-100", ITEM_NO="101", STORE="100"),
            Row(SECTION_MASTER="SM 202-100", ITEM_NO="202", STORE="100"),
            Row(SECTION_MASTER="SM 303-100", ITEM_NO="303", STORE="100"),
            Row(SECTION_MASTER="SM 101-200", ITEM_NO="101", STORE="200"),
            Row(SECTION_MASTER="SM 202-200", ITEM_NO="202", STORE="200"),
            Row(SECTION_MASTER="SM 303-200", ITEM_NO="303", STORE="200"),
            Row(SECTION_MASTER="SM 101-300", ITEM_NO="101", STORE="300"),
            Row(SECTION_MASTER="SM 202-300", ITEM_NO="202", STORE="300"),
            Row(SECTION_MASTER="SM 303-300", ITEM_NO="303", STORE="300"),
        ]
    )

    df_result = get_core_trans(
        df_trans=df_trans,
        df_banner_lookup=df_banner_lookup,
        df_prod_hierarchy=df_prod_hierarchy,
        df_item_pog_section_lookup=df_item_pog_section_lookup,
        banner=[],
        pog_section_list=[],
        st_date="2020-01-01",
        end_date="2020-01-01",
    )

    df_result.cache()

    assert df_result.count() == 3

    cols_expected = [
        "TRANSACTION_RK",
        "CALENDAR_DT",
        "RETAIL_OUTLET_LOCATION_SK",
        "STORE_NO",
        "ITEM_SK",
        "ITEM_NO",
        "CUSTOMER_SK",
        "CUSTOMER_CARD_SK",
        "REGION",
        "NATIONAL_BANNER_DESC",
        "SECTION_MASTER",
    ]

    check_invalid(df_result, cols_expected)

    df_result = get_core_trans(
        df_trans=df_trans,
        df_banner_lookup=df_banner_lookup,
        df_prod_hierarchy=df_prod_hierarchy,
        df_item_pog_section_lookup=df_item_pog_section_lookup,
        banner=["some banner"],
        pog_section_list=[],
        st_date="2020-01-01",
        end_date="2020-01-01",
    )

    assert df_result.limit(1).count() == 0

    df_result = get_core_trans(
        df_trans=df_trans,
        df_banner_lookup=df_banner_lookup,
        df_prod_hierarchy=df_prod_hierarchy,
        df_item_pog_section_lookup=df_item_pog_section_lookup,
        banner=[],
        pog_section_list=["SM 101-200"],
        st_date="2020-01-01",
        end_date="2020-01-01",
    )

    assert df_result.count() == 1


def test_add_customer_card_id(spark: SparkSession):

    df = spark.createDataFrame([Row(CUSTOMER_CARD_SK=11)])
    df_cust_reachable = spark.createDataFrame(
        [Row(CUSTOMER_CARD_SK=111, CUSTOMER_CARD_ID=1110, DROP_MODIFIED_DATE=0)]
    )

    df_result = add_customer_card_id(
        df=df,
        df_cust_reachable=df_cust_reachable,
    )

    assert df_result.count() == 1

    # test no match
    df = spark.createDataFrame([Row(CUSTOMER_CARD_SK=22)])
    df_cust_reachable = spark.createDataFrame(
        [Row(CUSTOMER_CARD_SK=111, CUSTOMER_CARD_ID=1110, DROP_MODIFIED_DATE=0)]
    )

    df_result = add_customer_card_id(
        df=df,
        df_cust_reachable=df_cust_reachable,
    )

    assert df_result.limit(1).count() == 0

    # test -2 card
    df = spark.createDataFrame([Row(CUSTOMER_CARD_SK=11)])

    df_cust_reachable = spark.createDataFrame(
        [Row(CUSTOMER_CARD_SK=111, CUSTOMER_CARD_ID=-2, DROP_MODIFIED_DATE=0)]
    )

    df_result = add_customer_card_id(
        df=df,
        df_cust_reachable=df_cust_reachable,
    )

    assert df_result.limit(1).count() == 0


def test_get_frequent_visitors(spark: SparkSession):
    df = spark.createDataFrame(
        [
            Row(CUSTOMER_CARD_ID=11, TRANSACTION_RK=1),
            Row(CUSTOMER_CARD_ID=11, TRANSACTION_RK=1),
            Row(CUSTOMER_CARD_ID=11, TRANSACTION_RK=2),
            Row(CUSTOMER_CARD_ID=22, TRANSACTION_RK=3),
            Row(CUSTOMER_CARD_ID=22, TRANSACTION_RK=4),
            Row(CUSTOMER_CARD_ID=22, TRANSACTION_RK=5),
        ]
    )

    # test for zero threshold (all records must remain)
    df_result = get_frequent_visitors(df=df, visit_thresh=0)

    assert df_result.count() == df.count()

    # test for 2 threshold (3 records must remain)`
    df_result = get_frequent_visitors(df=df, visit_thresh=2)

    assert df_result.count() == 3

    # test for 3 threshold (no records must remain)
    df_result = get_frequent_visitors(df=df, visit_thresh=3)

    assert df_result.count() == 0


def test_remove_low_volume_items(spark: SparkSession):

    df = spark.createDataFrame(
        [
            Row(CUSTOMER_CARD_ID=11, ITEM_NO=1),
            Row(CUSTOMER_CARD_ID=11, ITEM_NO=1),
            Row(CUSTOMER_CARD_ID=11, ITEM_NO=2),
            Row(CUSTOMER_CARD_ID=22, ITEM_NO=2),
            Row(CUSTOMER_CARD_ID=22, ITEM_NO=2),
            Row(CUSTOMER_CARD_ID=33, ITEM_NO=2),
        ]
    )

    df_result = remove_low_volume_items(df=df, item_vol_thresh=0)

    assert df.count() == df_result.count()

    df_result = remove_low_volume_items(df=df, item_vol_thresh=3)

    assert df_result.count() == 0


def test_get_items_diff_trans(spark: SparkSession):

    # testing same item in same transaction (must be empty)
    df = spark.createDataFrame(
        [
            Row(
                CUSTOMER_CARD_ID="1",
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                TRANSACTION_RK="1",
                ITEM_NO="1",
            ),
            Row(
                CUSTOMER_CARD_ID="1",
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                TRANSACTION_RK="1",
                ITEM_NO="1",
            ),
        ]
    )

    df_result = get_items_diff_trans(df)
    assert df_result.limit(1).count() == 0

    # testing same item in different transactions (must be empty)
    df = spark.createDataFrame(
        [
            Row(
                CUSTOMER_CARD_ID="1",
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                TRANSACTION_RK="1",
                ITEM_NO="1",
            ),
            Row(
                CUSTOMER_CARD_ID="1",
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                TRANSACTION_RK="2",
                ITEM_NO="1",
            ),
        ]
    )

    df_result = get_items_diff_trans(df)
    assert df_result.limit(1).count() == 0

    # testing different items in different transactions (must be empty)
    df = spark.createDataFrame(
        [
            Row(
                CUSTOMER_CARD_ID="1",
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                TRANSACTION_RK="1",
                ITEM_NO="1",
            ),
            Row(
                CUSTOMER_CARD_ID="1",
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                TRANSACTION_RK="2",
                ITEM_NO="2",
            ),
        ]
    )

    df_result = get_items_diff_trans(df)
    assert df_result.count() == 2


def test_get_items_lost_after_explosion(spark: SparkSession):

    # testing no items lost (should be empty)
    df_low_vol = spark.createDataFrame(
        [
            Row(
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                ITEM_NO="1",
            ),
            Row(
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                ITEM_NO="2",
            ),
        ]
    )

    df_cust_item_diff = spark.createDataFrame(
        [
            Row(
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                ITEM_A="1",
                ITEM_B="2",
            ),
            Row(
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                ITEM_A="1",
                ITEM_B="2",
            ),
        ]
    )

    df_result = get_items_lost_after_explosion(
        df_low_vol=df_low_vol,
        df_cust_item_diff=df_cust_item_diff,
    )

    assert df_result.limit(1).count() == 0

    # testing exactly 1 item lost
    df_low_vol = spark.createDataFrame(
        [
            Row(
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                ITEM_NO="1",
            ),
            Row(
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                ITEM_NO="2",
            ),
            Row(
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                ITEM_NO="3",
            ),
        ]
    )

    df_cust_item_diff = spark.createDataFrame(
        [
            Row(
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                ITEM_A="1",
                ITEM_B="2",
            ),
            Row(
                SECTION_MASTER="1",
                REGION="ontario",
                NATIONAL_BANNER_DESC="1",
                ITEM_A="1",
                ITEM_B="2",
            ),
        ]
    )

    df_result = get_items_lost_after_explosion(
        df_low_vol=df_low_vol,
        df_cust_item_diff=df_cust_item_diff,
    )

    data_result = df_result.collect()

    assert len(data_result) == 1
    assert data_result[0]["ITEM_PERC_LOST"] == 33.3
    assert data_result[0]["ITEM_LIST_LOST"] == ["3"]


def test_translate_spaceman(spark, df_spaceman_deduped):

    df_spaceman_deduped = spark.createDataFrame(
        [
            Row(
                STORE="1",
                FR_SECTION_MASTER="FR_SECTION_MASTER_C",
                SECTION_NAME="name c 3FT (123)",
                RELEASE_DATE="03Oct2021",
                ITEM_NO="111",
            ),
            Row(
                STORE="2",
                FR_SECTION_MASTER="FR_SECTION_MASTER_F",
                SECTION_NAME="name f 3FT (123)",
                RELEASE_DATE="06Oct2021",
                ITEM_NO="222",
            ),
            Row(
                STORE="3",
                FR_SECTION_MASTER="FR_SECTION_MASTER_ZZ",
                SECTION_NAME="name zz 3FT (123)",
                RELEASE_DATE="10Oct2021",
                ITEM_NO="333",
            ),
        ]
    )

    # must break with this one because it is not sufficient
    df_translations_sm = spark.createDataFrame(
        [
            Row(
                FR_SECTION_MASTER="FR_SECTION_MASTER_ZZ",
                SECTION_MASTER="TRANSLATED_SECTION_MASTER_ZZ",
                IN_SCOPE=1,
            ),
        ]
    )

    broke = False

    try:
        translate_spaceman(
            df_spaceman=df_spaceman_deduped,
            df_translations_sm=df_translations_sm,
        )
    except Exception:
        broke = True

    assert broke, "must break with insufficient (outdated) translation table"


def test_udf_impute_by_facings():

    pdf_spaceman_section = pd.DataFrame(
        [
            {
                "FR_SECTION_MASTER": "FR_SECTION_MASTER_C",
                "STORE": "1",
                "SECTION_NAME": "name c 3FT (123)",
                "RELEASE_DATE": "03Oct2021",
                "ITEM_NO": "111",
                "SECTION_MASTER": "TRANSLATED_SECTION_MASTER_ZZ",
                "FACINGS": 1,
            },
            {
                "FR_SECTION_MASTER": "FR_SECTION_MASTER_C",
                "STORE": "2",
                "SECTION_NAME": "name c 3FT (123)",
                "RELEASE_DATE": "03Oct2021",
                "ITEM_NO": "111",
                "SECTION_MASTER": "TRANSLATED_SECTION_MASTER_ZZ",
                "FACINGS": None,
            },
        ]
    )

    pdf_expected: pd.DataFrame = pd.DataFrame(
        [{"STORE": "2", "ITEM_NO": "111", "REF_FACINGS": 1.0}]
    )

    pdf_result = udf_impute_by_facings.func(pdf_spaceman_section=pdf_spaceman_section)
    assert all(pdf_expected == pdf_result)


def test_validate_using_exec_id(spark):

    # basic test case
    # should pass
    df = spark.createDataFrame([Row(SECTION_MASTER="ABC")])
    validate_using_exec_id(df=df)

    # test case with exact same SECTION_MASTER
    # should also pass
    df = spark.createDataFrame([Row(SECTION_MASTER="ABC"), Row(SECTION_MASTER="ABC")])
    validate_using_exec_id(df=df)

    # test case with COMPLETELY different SECTION_MASTER
    # should also pass
    df = spark.createDataFrame([Row(SECTION_MASTER="ABC"), Row(SECTION_MASTER="XYZ")])
    validate_using_exec_id(df=df)

    # test case with same SECTION_MASTER's by slightly different
    # should FAIL

    broke = False

    try:
        df = spark.createDataFrame(
            [Row(SECTION_MASTER="ABC"), Row(SECTION_MASTER="A BC")]
        )
        validate_using_exec_id(df=df)
    except AssertionError:
        broke = True

    assert broke


def test_add_case_pack_info(spark):

    # basic test case with only apollo slice
    df_pog = spark.createDataFrame([Row(STORE="S1", ITEM_NO="I1", SOURCE="APOLLO")])

    df_apollo = spark.createDataFrame([Row(STORE="S1", ITEM_NO="I1", CASEPACK="C1")])

    df = add_case_pack_info(
        df_pog=df_pog,
        df_apollo=df_apollo,
    )

    df.cache()

    assert df.count() == 1
    assert df.select("CASEPACK").collect()[0][0] == "C1"

    # basic test case with apollo+SPACEMAN slice
    df_pog = spark.createDataFrame(
        [
            Row(STORE="S1", ITEM_NO="I1", SOURCE="APOLLO"),
            Row(STORE="S2", ITEM_NO="I1", SOURCE="SPACEMAN"),
        ]
    )

    df_apollo = spark.createDataFrame([Row(STORE="S1", ITEM_NO="I1", CASEPACK="C1")])

    df = add_case_pack_info(
        df_pog=df_pog,
        df_apollo=df_apollo,
    )

    df.cache()

    assert df.count() == 2
    coll = df.select("CASEPACK").collect()
    assert coll[0]["CASEPACK"] == "C1"
    assert coll[1]["CASEPACK"] == "C1"
