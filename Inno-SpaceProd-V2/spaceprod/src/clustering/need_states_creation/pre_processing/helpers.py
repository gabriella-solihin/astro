import enum
import os
import traceback
from typing import List

import numpy as np
import pandas as pd

from inno_utils.loggers import log
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import Window
from spaceprod.src.clustering.need_states_creation.prod2vec.helpers import (
    generate_execution_id,
)
from spaceprod.src.utils import (
    dedup_product,
    process_location,
)
from spaceprod.utils.data_transformation import (
    apply_data_definition,
    df_to_dict,
    filter_df_to_invalid,
    is_col_null_mask,
    union_dfs,
)
from spaceprod.utils.imports import F, T
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.validation import check_lookup_coverage, dup_check


def dedup_apollo(
    df_apollo: SparkDataFrame,
) -> SparkDataFrame:
    """
    Cleans Apollo POG data by removing duplicate store-section values.

    Parameters
    ----------
    df_apollo: POG section dataset (external)

    Returns
    -------
    df_apollo: cleaned Apollo POG
    """

    ## 1. Take POG w/ latest release date
    df_apollo = df_apollo.filter(F.col("STATUS") == "Released")

    df_apollo = df_apollo.withColumn(
        "RELEASE_DATE_DT", F.to_timestamp(F.col("RELEASE_DATE"), "ddMMMyyyy")
    )

    df_apollo_max_date = df_apollo.groupBy(["STORE", "SECTION_MASTER"]).agg(
        F.max("RELEASE_DATE_DT").alias("RELEASE_DATE_DT")
    )

    dims = ["STORE", "RELEASE_DATE_DT", "SECTION_MASTER"]
    df_apollo_max_date = df_apollo_max_date.select(*dims)
    df_apollo = df_apollo.join(df_apollo_max_date, on=dims, how="inner")
    df_apollo = df_apollo.drop("RELEASE_DATE_DT")

    ## 2. Take POG that has the largest number of items
    df_apollo_pogs = df_apollo.groupBy("STORE", "SECTION_MASTER", "SECTION_NAME").agg(
        F.countDistinct("ITEM_NO").alias("ITEM_COUNT")
    )
    w = Window.partitionBy("STORE", "SECTION_MASTER")
    df_apollo_pogs = df_apollo_pogs.withColumn(
        "MAX_ITEM_COUNT", F.max("ITEM_COUNT").over(w)
    )
    df_max_pogs = (
        df_apollo_pogs.where(F.col("ITEM_COUNT") == F.col("MAX_ITEM_COUNT"))
        .drop("MAX_ITEM_COUNT")
        .drop("ITEM_COUNT")
    )
    df_apollo_cleaned = df_apollo.join(
        df_max_pogs, on=["STORE", "SECTION_MASTER", "SECTION_NAME"]
    )
    df_apollo_cleaned = df_apollo_cleaned.dropDuplicates(
        ["STORE", "SECTION_MASTER", "SECTION_NAME", "ITEM_NO"]
    )

    ## 3. For all the remaining POGs that have the same section master, release date, and number of items (~0.2% of all POGS), just take one of the sections arbitrarily.
    # TODO - come up with a better solution
    # Do so deterministically by adding a ranking.
    window = Window.partitionBy(["STORE", "SECTION_MASTER"]).orderBy(
        ["SECTION_NAME", "ITEM_NO"]
    )
    df_apollo_cleaned = df_apollo_cleaned.withColumn("rank", F.rank().over(window))
    df_one_section_name = df_apollo_cleaned.filter(F.col("rank") == 1).select(
        "STORE", "SECTION_MASTER", "SECTION_NAME"
    )
    df_apollo_deduped = df_apollo_cleaned.join(
        df_one_section_name, on=["STORE", "SECTION_MASTER", "SECTION_NAME"]
    ).drop("rank")

    ## 4. Deduplicate on STORE-ITEM
    df_apollo_deduped = df_apollo_deduped.dropDuplicates(["STORE", "ITEM_NO"])

    return df_apollo_deduped


def dedup_spaceman(
    df_spaceman: SparkDataFrame,
) -> SparkDataFrame:
    """
    Cleans Spaceman POG data by taking the latest store-section.

    Parameters
    ----------
    df_spaceman: POG section dataset (external)

    Returns
    -------
    df_spaceman: cleaned Spaceman POG
    """
    df_spaceman_max_date = df_spaceman.groupBy(["STORE", "FR_SECTION_MASTER"]).agg(
        F.max("RELEASE_DATE").alias("RELEASE_DATE")
    )

    dims = ["STORE", "RELEASE_DATE", "FR_SECTION_MASTER"]
    df_spaceman_max_date = df_spaceman_max_date.select(*dims)
    df_spaceman = df_spaceman.join(df_spaceman_max_date, on=dims, how="inner")

    ## 2. Take POG that has the largest number of items
    df_spaceman_pogs = df_spaceman.groupBy(
        "STORE", "FR_SECTION_MASTER", "SECTION_NAME"
    ).agg(F.countDistinct("ITEM_NO").alias("ITEM_COUNT"))
    w = Window.partitionBy("STORE", "FR_SECTION_MASTER")
    df_spaceman_pogs = df_spaceman_pogs.withColumn(
        "MAX_ITEM_COUNT", F.max("ITEM_COUNT").over(w)
    )
    df_max_pogs = (
        df_spaceman_pogs.where(F.col("ITEM_COUNT") == F.col("MAX_ITEM_COUNT"))
        .drop("MAX_ITEM_COUNT")
        .drop("ITEM_COUNT")
    )
    df_spaceman_cleaned = df_spaceman.join(
        df_max_pogs, on=["STORE", "FR_SECTION_MASTER", "SECTION_NAME"]
    )
    df_spaceman_cleaned = df_spaceman_cleaned.dropDuplicates(
        ["STORE", "FR_SECTION_MASTER", "SECTION_NAME", "ITEM_NO"]
    )

    ## 3. For all the remaining POGs that have the same section master, release date, and number of items (~0.2% of all POGS), just take one of the sections arbitrarily.
    # TODO - come up with a better solution
    window = Window.partitionBy(["STORE", "FR_SECTION_MASTER"]).orderBy(
        ["SECTION_NAME", "ITEM_NO"]
    )
    df_spaceman_cleaned = df_spaceman_cleaned.withColumn("rank", F.rank().over(window))
    df_one_section_name = df_spaceman_cleaned.filter(F.col("rank") == 1).select(
        "STORE", "FR_SECTION_MASTER", "SECTION_NAME"
    )
    df_spaceman_deduped = df_spaceman_cleaned.join(
        df_one_section_name, on=["STORE", "FR_SECTION_MASTER", "SECTION_NAME"]
    ).drop("rank")

    ## 4. Deduplicate on STORE-ITEM
    df_spaceman_deduped = df_spaceman_deduped.dropDuplicates(["STORE", "ITEM_NO"])

    return df_spaceman_deduped


def clean_section_master(
    df_apollo: SparkDataFrame,
) -> SparkDataFrame:
    """
    Creates an item<->pog section dim table to be able to map items and
    transactions to pog sections. Used in transaction data to fileter to
    the right scope.

    Parameters
    ----------
    df_apollo: POG section dataset (external)

    Returns
    -------
    item<->pog section mapping table
    """

    df = df_apollo

    # repartition data across nodes
    n_nodes = spark.sparkContext._jsc.sc().getExecutorMemoryStatus().size() - 1
    if n_nodes > 0:
        df = df.repartition(n_nodes)
    elif os.getenv("SPACE_REPARTITION_COUNT") is not None:
        n_repartition_count = int(os.getenv("SPACE_REPARTITION_COUNT"))
        df = df.repartition(n_repartition_count)

    # first make sure there is no invalid records for SECTION_NAME
    mask_invalid = is_col_null_mask("SECTION_NAME")
    df = df.filter(~mask_invalid)

    # now trim the SECTION_NAME to remove redundant white space
    df = df.withColumn("SECTION_NAME", F.trim(F.col("SECTION_NAME")))

    # next only ensure we only include SECTION_NAME's that start with a letter
    # because there might be some invalid ones
    # TODO: we might get some here (no more than 0.5% of total row count)
    #  because SECTION_NAME for them either does not have a number or it
    #  starts with a number (i.e. it does not comply with the below logic)
    #  but it is a very low ~ 0.5% (less than a percent) (50862/9795150)
    mask = F.substring(F.col("SECTION_NAME"), 0, 1).rlike("[A-Za-z]")
    df = df.filter(mask)

    # next we only include SECTION_NAME's that contain a digit
    # this is also 99% of what we see
    mask = F.col("SECTION_NAME").rlike("[0-9]")
    df = df.filter(mask)

    # add section master by extracting it from the section name
    # first we find the first digit that occurs from the left of the string
    exp_first_digit = "regexp_extract(SECTION_NAME, '([0-9]+)', 1)"
    # next we find the position of that digit from the left
    exp_first_position = f"locate({exp_first_digit}, SECTION_NAME) - 1"
    # next we substring everything from the left until that position
    exp_section_master = f"initcap(substring(SECTION_NAME, 0, {exp_first_position}))"
    # it becomes our preliminary SECTION_MASTER
    df = df.select("*", F.expr(exp_section_master).alias("SECTION_MASTER"))

    df.persist()

    # now make sure we don't have any invalid SECTION_MASTER vals at this point
    mask_invalid = is_col_null_mask("SECTION_MASTER")
    df_check = df.filter(mask_invalid)
    msg = "We should not be getting invalid SECTION_MASTER at this point"
    assert df_check.limit(1).count() == 0, msg

    # SECTION_MASTER that was just extracted from section name
    # contains inconsistent spelling for the same section master, for e.g.:
    # "Baby Food, Formula" vs "Baby Food Formula"
    # lets fix this
    # first replace all non-alphanumeric with spaces
    col_letters = F.regexp_replace(F.col("SECTION_MASTER"), "[^0-9A-Za-z]+", " ")
    # next remove redundant spacing
    col_strip = F.trim(col_letters)
    # than make sure the case is consistent
    col_consistent = F.initcap(F.lower(col_strip))
    df = df.withColumn("SECTION_MASTER", col_consistent)

    # we are now done with adding SECTION_MASTER

    # ITEM_NO == STOCKCODE
    df = df.withColumnRenamed("STOCKCODE", "ITEM_NO")

    ###########################################################################
    # REMOVE STOP WORDS
    ###########################################################################

    # remove common words in section name that do not possess section
    # information these include things like the banner, region, and common stop

    # there are 2 kind of stop words: whole words and partial words

    # these stop words will removed, i.e. replaced with space (" ") only
    # if a WHOLE word is matched
    stop_words_whole_words = [
        "fc",
        "sby",
        "rack",
        "ont",
        "atl",
        "sfwy",
        "ver",
        "fdld",
        "fdl",
        '"',
        "sobeys",
        "foodland",
        "and",
    ]

    # these stop words will removed, i.e. replaced with space (" ") even
    # when matched partially (i.e. within a word)
    stop_words_partial = ["_"]

    # create a list of matching pattern for whole words
    list_ptn_whole = [f"(\\b{x}\\b)" for x in stop_words_whole_words]

    # create a list of matching pattern for partial words
    list_ptn_partial = [f"({x})" for x in stop_words_partial]

    # create a list of all patterns
    list_ptn = list_ptn_whole + list_ptn_partial

    # complete the pattern and apply it
    ptn = "^.*" + "|".join(list_ptn) + ".*$"
    col_new = F.regexp_replace(F.lower("SECTION_MASTER"), ptn, " ")
    df = df.withColumn("SECTION_MASTER", col_new)

    ###########################################################################
    # REPLACE INCONSISTENT KEY-WORDS
    ###########################################################################

    # the following are not stop words, but just replacement of
    # 'fe' abbriviation to 'front end'
    # words.we also replace underscores with space, and the abbrevation fe to
    # front end.
    ptn = "^.*(\\bfe\\b).*$"
    col_new = F.regexp_replace(F.col("SECTION_MASTER"), ptn, "front end")
    df = df.withColumn("SECTION_MASTER", col_new)

    ###########################################################################
    # REMOVE SPACES + CONVERT TO "INITCAP"
    ###########################################################################

    # `initcap` + space removal is needed to be able to NOT distinguish
    # between 'sidedish' and 'side dish' when we generate EXEC_ID's later
    col = F.col("SECTION_MASTER")
    col = F.initcap(F.regexp_replace(F.trim(col), " ", ""))
    df = df.withColumn("SECTION_MASTER", col)

    ###########################################################################
    # FILTER OUT ANY INVALID SECTION MASTERS
    ###########################################################################

    df.persist()

    # at this point we might still have some invalid SMs
    mask = is_col_null_mask("SECTION_MASTER")
    df_check = df.filter(mask)
    n_invalid_row = df_check.count()
    dict_invalid_names = df_to_dict(df_check.select("SECTION_NAME").dropDuplicates())
    list_invalid_name = list(set([list(x.values())[0] for x in dict_invalid_names]))
    df = df.filter(~mask)

    if n_invalid_row > 0:

        msg = f"""
        After removing stop words and the inital clean up of SECTION_MASTER 
        The following {len(list_invalid_name)} of SECTION_NAME's
        yielded invalid SECTION_MASTER's
        thus were filtered out (or {n_invalid_row} rows):
        {list_invalid_name}
        """

        log.info(msg)
    else:
        msg = f"""
        After removing stop words and the inital clean up of SECTION_MASTER
        we did not find any SECTION_MASTER's
        """

        log.info(msg)

    df.unpersist()
    return df


def filter_apollo(df_apollo):
    """
    Filter Apollo of dummy stores

    Parameters
    ----------
    df_spaceman: Apollo POG data

    Returns
    -------
    Filtered apollo POG
    """
    bad_stores = [1, 6, 999, 9999]
    df_apollo = df_apollo.filter(~F.col("STORE").isin(bad_stores))

    return df_apollo


def clean_spaceman(df_spaceman: SparkDataFrame) -> SparkDataFrame:
    """Clean item number column of the spaceman POG, get French section masters, as well as align relase dates.

    Parameters
    ----------
    df_spaceman: Spaceman POG data

    Returns
    -------
    cleaned spaceman POG
    """
    df_spaceman = df_spaceman.withColumnRenamed("STOCKCODE", "ITEM_NO")
    df_spaceman = df_spaceman.withColumnRenamed(
        "SobeysPlanoPerso_ArticleNmbr", "ITEM_NO"
    )
    df_spaceman = df_spaceman.withColumn(
        "ITEM_NO", F.regexp_replace(F.col("ITEM_NO"), "^0*", "")
    )
    df_spaceman = df_spaceman.withColumn("ITEM_NO", F.trim("ITEM_NO"))
    df_spaceman = df_spaceman.withColumn(
        "ITEM_NO", F.col("ITEM_NO").cast(T.IntegerType()).cast(T.StringType())
    )

    df_spaceman = df_spaceman.withColumn(
        "RELEASE_DATE", F.date_format(F.col("RELEASE_DATE"), "ddMMMyyyy")
    )

    df_spaceman = df_spaceman.withColumn(
        "FR_SECTION_MASTER", F.regexp_replace(F.col("SECTION_NAME"), "_", " ")
    )
    df_spaceman = df_spaceman.withColumn(
        "FR_SECTION_MASTER",
        F.regexp_extract(
            F.col("FR_SECTION_MASTER"),
            """(EPI |SUR |LAI |RB |F\&L |BS |\d+ |SURGELE )+(.+?) \d+""",
            2,
        ),
    )

    return df_spaceman


def translate_spaceman(df_spaceman: SparkDataFrame, df_translations_sm: SparkDataFrame):
    """
    Translate spaceman FR section masters to English via a manually translated lookup table.

    Parameters
    ----------
    df_spaceman: Spacaeman POG data
    df_translations: translation file for section masters that cannot be found

    Returns
    -------
    Translated spaceman POG
    """

    cols = ["FR_SECTION_MASTER", "SECTION_MASTER", "IN_SCOPE"]
    df_translations_sm = df_translations_sm.select(*cols)

    df_spaceman_full_eng = df_spaceman.join(
        other=df_translations_sm,
        on="FR_SECTION_MASTER",
        how="left",
    )

    check_lookup_coverage(
        df=df_spaceman_full_eng,
        col_looked_up="IN_SCOPE",
        dataset_id_of_external_data="french_to_english_translations",
        threshold=0,
    )

    # Check to ensure that no more than 1% of POGs get dropped
    df_spaceman_missing = df_spaceman_full_eng.filter(F.col("SECTION_MASTER").isNull())

    cols = ["STORE", "FR_SECTION_MASTER"]
    num_missing_store_pogs = df_spaceman_missing.dropDuplicates(cols).count()

    num_missing_pogs = df_spaceman_missing.dropDuplicates(["FR_SECTION_MASTER"]).count()

    total_store_pogs = df_spaceman_full_eng.dropDuplicates(
        ["STORE", "FR_SECTION_MASTER"]
    ).count()
    total_pogs = df_spaceman_full_eng.dropDuplicates(["FR_SECTION_MASTER"]).count()
    ratio_missing_store_pogs = num_missing_store_pogs / total_store_pogs
    ratio_missing_pogs = num_missing_pogs / total_pogs
    log.info(
        f"% of QC store-sections unaccounted for after manual mapping: {ratio_missing_store_pogs * 100}"
    )
    log.info(
        f"% of QC section masters unaccounted for after manual mapping: {ratio_missing_pogs * 100}"
    )
    if ratio_missing_store_pogs > 0.01 or ratio_missing_pogs > 0.01:
        raise Exception(
            "More than 1\% of POGs are unaccounted for in Quebec Canada. Please update the manual mapping file."
        )

    # Only return sections in scope
    df_spaceman_to_return = df_spaceman_full_eng.filter(F.col("IN_SCOPE") == 1)
    df_spaceman_to_return = df_spaceman_to_return.drop("IN_SCOPE")
    return df_spaceman_to_return


schema_udf_impute_by_facings = T.StructType(
    [
        T.StructField("STORE", T.IntegerType()),
        T.StructField("ITEM_NO", T.StringType()),
        T.StructField("REF_FACINGS", T.IntegerType()),
    ]
)


@F.pandas_udf(schema_udf_impute_by_facings, F.PandasUDFType.GROUPED_MAP)
def udf_impute_by_facings(pdf_spaceman_section):

    try:

        def find_most_similar_store(df):
            facing_diff = np.sum(np.abs(df["FACINGS"] - df["REF_FACINGS"]))
            return pd.DataFrame(
                {"item_overlap": [df.shape[0]], "facing_diff": facing_diff}
            )

        stores_with_missing_facings = pdf_spaceman_section[
            pdf_spaceman_section.FACINGS.isna()
        ].STORE.unique()

        # this is the 'bad' POG data that we need to impute
        spaceman_with_missing_stores = pdf_spaceman_section[
            pdf_spaceman_section.STORE.isin(stores_with_missing_facings)
        ]

        # this is the 'good' reference POG data that we 'look up' with
        spaceman_with_non_missing_stores = pdf_spaceman_section[
            ~pdf_spaceman_section.STORE.isin(stores_with_missing_facings)
        ]

        # the null facing records
        spaceman_missing_stores_bad = spaceman_with_missing_stores[
            spaceman_with_missing_stores.FACINGS.isna()
        ]

        spaceman_with_missing_stores = spaceman_with_missing_stores[
            ["STORE", "ITEM_NO", "FACINGS"]
        ]
        spaceman_with_non_missing_stores = spaceman_with_non_missing_stores[
            ["STORE", "ITEM_NO", "FACINGS"]
        ]
        spaceman_with_non_missing_stores = spaceman_with_non_missing_stores.rename(
            {"STORE": "REF_STORE", "FACINGS": "REF_FACINGS"}, axis=1
        )

        # every pairwise combination of two stores (one in good, one in bad) for every item
        item_cross = spaceman_with_missing_stores.merge(
            spaceman_with_non_missing_stores, on=["ITEM_NO"]
        )

        # either one the left or right joined table has 0 rows, or there is 0 item overlap. In this case, we cannot impute.
        if item_cross.shape[0] == 0:
            return pd.DataFrame(
                {
                    "STORE": pd.Series([], dtype="int"),
                    "ITEM_NO": pd.Series([], dtype="str"),
                    "REF_FACINGS": pd.Series([], dtype="int"),
                }
            )
        # Get every store-pair (one in good, one in bad) with their similarity metrics (item overlap, facing variation distance) calculated
        result = pd.DataFrame(
            item_cross.groupby(["STORE", "REF_STORE"]).apply(find_most_similar_store)
        ).reset_index()

        # Sort the store pairs by similarity
        result = result.sort_values(
            ["STORE", "item_overlap", "facing_diff"], ascending=(True, False, True)
        )

        most_similar_store = (
            result.groupby("STORE").agg({"REF_STORE": "first"}).reset_index()
        )
        spaceman_with_non_missing_stores = spaceman_with_non_missing_stores.rename(
            {"STORE": "REF_STORE"}, axis=1
        )

        # Now join the nan facings with the most similar store, then with its number of facings.
        lookup = spaceman_missing_stores_bad.merge(
            most_similar_store, on=["STORE"]
        ).merge(spaceman_with_non_missing_stores, on=["REF_STORE", "ITEM_NO"])
        imputed_facings = lookup[["STORE", "ITEM_NO", "REF_FACINGS"]]
        return imputed_facings
    except:
        tb = traceback.format_exc()
        section_name = pdf_spaceman_section.SECTION_NAME.iloc[0]
        msg = f"""
                    Imputing spaceman facings broke for section_name: {section_name} 
                    execution threw an error:\n{tb}
                    \nFailed to run task for scope: {section_name} (see above).
                    """
        log.info(msg)
        raise Exception(msg)


def impute_spaceman_null_facings(df_spaceman: SparkDataFrame):
    """
    Impute the null facings of spaceman. Approximately 40% of records in the raw spaceman dataset have null facings.
    We conduct this imputation by looking for the most similar section with no null facings, and borrowing its facing value for the same item, should it exist.
    The most similar section is defined to be the section with the highest item overlap, tie broken by the total variation distance in facings across items.
    This process allows us to recover ~40% of all missing facings.

    Parameters
    ----------
    df_spaceman: Spacaeman POG data

    Returns
    -------
    Spaceman POG with facings imputed
    """

    # Here, we group by section name instead of section master.
    # This is because we consider POGs with different section names to have inherently different 'layouts', and should not use them for imputation.
    # e.g. BISCOTTES 12P is not the same as BISCOTTES 8P, and we shouldn't impute the facings of 8P using values from 12P.
    spaceman_imputed = df_spaceman.groupby("SECTION_NAME").apply(udf_impute_by_facings)
    df_spaceman = df_spaceman.join(
        spaceman_imputed, on=["STORE", "ITEM_NO"], how="left"
    )
    df_spaceman_imputed = df_spaceman.withColumn(
        "FACINGS",
        F.when(F.col("FACINGS").isNull(), F.col("REF_FACINGS")).otherwise(
            F.col("FACINGS")
        ),
    )
    df_spaceman_imputed = df_spaceman_imputed.drop("REF_FACINGS")
    return df_spaceman_imputed


def merge_pogs(
    df_apollo: SparkDataFrame,
    df_spaceman: SparkDataFrame,
) -> SparkDataFrame:
    """
    Combine apollo and spaceman POG dataframes.

    Parameters
    ----------
    df_spaceman: Spacaeman POG data
    df_apollo: Apollo POG data

    Returns
    -------
    merged POG
    """

    cols_apollo = [
        ("STORE", "STORE", T.StringType()),
        ("ITEM_NO", "ITEM_NO", T.StringType()),
        ("SECTION_NAME", "SECTION_NAME", T.StringType()),
        ("SECTION_MASTER", "SECTION_MASTER", T.StringType()),
        ("FACINGS", "FACINGS", T.IntegerType()),
        ("WIDTH", "WIDTH", T.DoubleType()),
        ("HEIGHT", "HEIGHT", T.DoubleType()),
        ("DEPTH", "DEPTH", T.DoubleType()),
        ("LENGTH_SECTION_INCHES", "LENGTH_SECTION_INCHES", T.IntegerType()),
        ("RELEASE_DATE", "RELEASE_DATE", T.StringType()),
    ]

    cols_spaceman = [
        ("STORE", "STORE", T.StringType()),
        ("ITEM_NO", "ITEM_NO", T.StringType()),
        ("SECTION_NAME", "SECTION_NAME", T.StringType()),
        ("SECTION_MASTER", "SECTION_MASTER", T.StringType()),
        ("FACINGS", "FACINGS", T.IntegerType()),
        ("WIDTH", "WIDTH", T.DoubleType()),
        ("HEIGHT", "HEIGHT", T.DoubleType()),
        ("DEPTH", "DEPTH", T.DoubleType()),
        ("LENGTH_SECTION_INCHES", "LENGTH_SECTION_INCHES", T.IntegerType()),
        ("RELEASE_DATE", "RELEASE_DATE", T.StringType()),
    ]

    df_a = apply_data_definition(
        df=df_apollo,
        data_contract=cols_apollo,
        drop_if_not_in_data_contract=True,
    )

    df_s = apply_data_definition(
        df=df_spaceman,
        data_contract=cols_spaceman,
        drop_if_not_in_data_contract=True,
    )

    # ensure that no store-item combo is in both POGs
    df_shared = df_a.join(df_s, on=["STORE"], how="inner")
    assert df_shared.count() == 0, "There are duplicate store numbers in both POGs"

    # add the source column
    df_a = df_a.withColumn("SOURCE", F.lit("APOLLO"))
    df_s = df_s.withColumn("SOURCE", F.lit("SPACEMAN"))

    df_pog = union_dfs([df_a, df_s], align_schemas=True)

    # Drop NAN facings
    df_pog = df_pog.filter(~F.col("FACINGS").isNull())

    return df_pog


def patch_pog(
    df_pog: SparkDataFrame,
    df_sm_override: SparkDataFrame,
    df_location: SparkDataFrame,
    thresh_ratio_missing_store_pogs: float,
    thresh_ratio_missing_pogs: float,
) -> SparkDataFrame:
    """
    update POG sections from a manual mapping file
    """

    # take the latest record for each STORE_NO location to be able
    # to lookup region/banner info since the override data is on that level
    # we also filter out invalid records where applicable
    df_loc = process_location(df_location, ["STORE_NO"])

    # select relevant columns from location
    cols = [
        F.col("STORE_NO").alias("STORE"),
        F.lower(F.col("REGION_DESC")).alias("Region"),
        F.upper(F.col("NATIONAL_BANNER_DESC")).alias("Banner"),
    ]

    df_loc = df_loc.select(*cols)
    df_pog = df_pog.join(df_loc, "STORE", "left")

    # select relevant columns from the override table
    cols = [
        F.lower(F.col("Region")).alias("Region"),
        F.upper(F.col("Banner")).alias("Banner"),
        F.col("SECTION_MASTER"),
        F.col("UPDATED_SECTION_MASTER"),
    ]

    df_sm_override = df_sm_override.select(*cols)
    dup_check(df_sm_override, ["Region", "Banner", "SECTION_MASTER"])

    # override with a "hard" Section Master from a specific "override" file
    dims = ["Region", "Banner", "SECTION_MASTER"]
    df_pog = df_pog.join(other=df_sm_override, on=dims, how="left")

    # Check to ensure that no more than 15% of POGs get dropped.
    # Note that this is high because it includes the sections
    # that are not in scope (e.g. vitamins)
    df_pog = df_pog.persist()
    df_pog_missing = df_pog.filter(F.col("UPDATED_SECTION_MASTER").isNull())

    # perform various counts
    cols = ["STORE", "SECTION_MASTER"]
    num_missing_store_pogs = df_pog_missing.dropDuplicates(cols).count()
    num_missing_pogs = df_pog_missing.dropDuplicates(["SECTION_MASTER"]).count()
    total_store_pogs = df_pog.dropDuplicates(["STORE", "SECTION_MASTER"]).count()
    total_pogs = df_pog.dropDuplicates(["SECTION_MASTER"]).count()

    # Last check: 6.36%
    ratio_missing_store_pogs = num_missing_store_pogs / total_store_pogs

    # Last check: 12.57%
    ratio_missing_pogs = num_missing_pogs / total_pogs

    msg = f"""
    % of EC store-sections unaccounted for after manual mapping: {ratio_missing_store_pogs * 100}
    % of EC section masters unaccounted for after manual mapping: {ratio_missing_pogs * 100}
    """

    log.info(msg)

    exceeds_store_pogs = ratio_missing_store_pogs > thresh_ratio_missing_store_pogs
    exceeds_pogs = ratio_missing_pogs > thresh_ratio_missing_pogs

    if exceeds_store_pogs or exceeds_pogs:
        msg = """
        More than 15% of POGs are unaccounted for in English Canada. 
        Please update the manual mapping file.
        """

        raise Exception(msg)

    # drop all front end items
    mask = ~(F.col("UPDATED_SECTION_MASTER") == "FE CONFECTIONARY")
    df_pog = df_pog.filter(mask)
    mask = ~(F.col("UPDATED_SECTION_MASTER") == "FRONTEND - COOLER COMBO")
    df_pog = df_pog.filter(mask)

    # override the 'SECTION_MASTER' column if required
    col_sm_u = F.col("UPDATED_SECTION_MASTER")
    mask = col_sm_u.isNull()
    col_sm = F.when(mask, F.col("SECTION_MASTER")).otherwise(col_sm_u)
    df_pog = df_pog.withColumn("SECTION_MASTER", col_sm)
    df_pog = df_pog.drop("UPDATED_SECTION_MASTER")

    df_pog.unpersist()

    return df_pog


def get_consistent_item_widths(df_pog: SparkDataFrame) -> SparkDataFrame:
    """
    Make the width of each item across stores consistent.

    From analysis, about 30% of items do not have the same width across stores.
    Currently, we conservatively take the max width of an item across stores as the width.

    Parameters
    ----------
    df_pog: combined POG data

    Returns
    -------
    POG dataframe with consistent widths
    """
    df_pog = df_pog.withColumn("WIDTH", F.round(F.col("WIDTH"), 2))
    df_pog_max_weight = df_pog.groupBy("ITEM_NO").agg(
        F.max(F.col("WIDTH")).alias("WIDTH")
    )
    df_pog = df_pog.drop("WIDTH").join(df_pog_max_weight, on="ITEM_NO", how="left")
    return df_pog


def get_section_master_modes(
    df_pog: SparkDataFrame,
    df_location: SparkDataFrame,
) -> SparkDataFrame:
    """
    Only keep items that have section master consistent with the mode section
    master of all items in the region banner.

    Parameters
    ----------
    df_pog: combined POG data

    Returns
    -------
    POG dataframe with section masters overridden
    """

    cols = [
        F.col("REGION_DESC").alias("REGION"),
        F.col("NATIONAL_BANNER_DESC").alias("BANNER"),
        F.col("STORE_NO"),
    ]

    df_location = process_location(df_location, ["STORE_NO"]).select(*cols)

    df_pog = df_pog.withColumnRenamed("STORE", "STORE_NO")
    df_pog_w_location = df_pog.join(other=df_location, on="STORE_NO", how="inner")

    # making the SM consistent between ALL stores for each SECTION_NAME within
    # each region/banner slice
    dims = ["REGION", "BANNER", "ITEM_NO"]
    col_mode = mode(F.collect_list(F.col("SECTION_MASTER")))
    agg = [col_mode.alias("MODE_SECTION_MASTER")]
    df_modes = df_pog_w_location.groupBy(dims).agg(*agg)

    df_pog_override = df_pog_w_location.join(df_modes, on=dims, how="inner")

    df_pog_mode_sections = df_pog_override.filter(
        F.col("MODE_SECTION_MASTER") == F.col("SECTION_MASTER")
    )
    df_pog_mode_sections = df_pog_mode_sections.drop(
        "REGION", "BANNER", "MODE_SECTION_MASTER"
    )
    df_pog_mode_sections = df_pog_mode_sections.withColumnRenamed("STORE_NO", "STORE")
    return df_pog_mode_sections


def combine_pogs_into_section_masters(df_pog: SparkDataFrame):
    """
    Combine POG sections within each store-section master due to mismatches
    in granularity between POG section and section master.
    POGs are combined if there is no item overlap between the 2 sections under
    the same section master. Otherwise, they are dropped.

    Parameters
    ----------
    df_pog: processed POG data, so far

    Returns
    -------
    df_pog_combined: POG dataframe at the store-section master level with different sections combined
    pog_by_sm: store-POGs that were either combined or dropped (may be used for debugging)
    """

    # 1. First we get all many to 1 and 1 to many mappings between POG section and section masters.
    @F.udf(returnType=T.MapType(T.StringType(), T.IntegerType()))
    def collect_counts(arr):
        import collections

        return dict(collections.Counter(arr))

    # create a DF containing counts of section names by store/SM
    agg = [
        collect_counts(F.collect_list("SECTION_NAME")).alias("SECTION_NAME_COUNTS"),
        F.countDistinct("SECTION_NAME").alias("NUMBER_OF_SECTIONS"),
    ]

    dims = ["STORE", "SECTION_MASTER"]
    pog_by_sm = df_pog.groupBy(*dims).agg(*agg)

    # create a DF containing counts of SMs by store/section_name
    agg = [
        collect_counts(F.collect_list("SECTION_MASTER")).alias("SECTION_MASTER_COUNTS"),
        F.countDistinct("SECTION_MASTER").alias("NUMBER_OF_SECTION_MASTERS"),
    ]

    dims = ["STORE", "SECTION_NAME"]
    pog_by_pog = df_pog.groupBy(*dims).agg(*agg)

    df_check = pog_by_pog.filter(F.col("NUMBER_OF_SECTION_MASTERS") > 1)
    msg = "There should not be any 1 to many (POG section name -> section master) POG sections."
    assert df_check.limit(1).count() == 0, msg

    # 2. Next, we combine the POGs.
    @F.udf(returnType=T.IntegerType())
    def item_set_diff(item_sets):
        """find every set intersection in item_sets, a list of sets."""
        set_diff_counts = []
        for i, item_set in enumerate(item_sets):
            for item_set_comp in item_sets[i + 1 :]:
                item_set = set(item_set)
                item_set_comp = set(item_set_comp)
                set_overlap = len(item_set.intersection(item_set_comp))
                set_diff_counts.append(set_overlap)
        return sum(set_diff_counts)

    # Get data to be at POG level
    df_pogs = df_pog.groupBy(["STORE", "SECTION_MASTER", "SECTION_NAME"]).agg(
        F.collect_set("ITEM_NO").alias("ITEMS"),
        F.first("LENGTH_SECTION_INCHES").alias("LENGTH_SECTION_INCHES"),
    )

    # Combine POGs at section master level, also calculating the item overlap between POGs.
    df_sm = df_pogs.groupBy(["STORE", "SECTION_MASTER"]).agg(
        F.concat_ws(" + ", F.collect_list("SECTION_NAME")).alias("CONCAT_SECTION_NAME"),
        item_set_diff(F.collect_list("ITEMS")).alias("ITEM_SET_DIFF"),
        F.sum("LENGTH_SECTION_INCHES").alias("SUM_LENGTH_SECTION_INCHES"),
        F.countDistinct("SECTION_NAME").alias("NUMBER_OF_SECTIONS"),
    )
    # Only keep POGs that do not have item overlap
    df_non_overlapped_pogs = df_sm.filter(F.col("ITEM_SET_DIFF") == 0).select(
        "STORE", "SECTION_MASTER", "CONCAT_SECTION_NAME", "SUM_LENGTH_SECTION_INCHES"
    )
    num_overlapped_pogs = df_sm.filter(F.col("ITEM_SET_DIFF") > 0).count()
    num_mult_pogs_per_sm = df_sm.filter(F.col("NUMBER_OF_SECTIONS") > 1).count()
    log.info(
        f"There are {num_overlapped_pogs} store-section masters that have multiple POGs (out of {num_mult_pogs_per_sm}) with overlapped items"
    )

    # Join back store-POG level data to the original store-item level POG.
    df_pog_combined = df_pog.join(
        df_non_overlapped_pogs, on=["STORE", "SECTION_MASTER"]
    )
    df_pog_combined = df_pog_combined.drop("SECTION_NAME", "LENGTH_SECTION_INCHES")
    df_pog_combined = df_pog_combined.withColumnRenamed(
        "CONCAT_SECTION_NAME", "SECTION_NAME"
    ).withColumnRenamed("SUM_LENGTH_SECTION_INCHES", "LENGTH_SECTION_INCHES")
    return df_pog_combined, pog_by_sm


def get_core_trans(
    df_trans: SparkDataFrame,
    df_banner_lookup: SparkDataFrame,
    df_prod_hierarchy: SparkDataFrame,
    df_item_pog_section_lookup: SparkDataFrame,
    banner: List[str],
    pog_section_list: List[str],
    st_date: str,
    end_date: str,
) -> SparkDataFrame:
    """
    creates a core transactions dataset by customer and item for the
    specified banner and category and the required time period

    Parameters
    ----------
    df_trans: raw transactions data (txnitem), i.e. external fact table
    df_banner_lookup: a dataset used to lookup banner by store ID
    df_prod_hierarchy: product dim table (external)
    df_item_pog_section_lookup: a POG dim table (external)
    banner: banner list in scope
    pog_section_list: list of POG sections in scope
    st_date: transaction data after this date will be include
    end_date: transaction data before this date will be include

    Returns
    -------
    trans_df: SparkDataFrame
    DataFrame containing the transactions for input to the model
    """

    # add SM + filter to scope in terms of SM if needed
    df_trans = lookup_section_master(
        df_txnitem=df_trans,
        pog_section_list=pog_section_list,
        df_item_pog_section_lookup=df_item_pog_section_lookup,
        df_product=df_prod_hierarchy,
        df_location=df_banner_lookup,
    )

    # filter to scope in terms of dates
    df_trans = pre_process_txnitem(
        df_txnitem=df_trans,
        st_date=st_date,
        end_date=end_date,
    )

    mask = F.col("SECTION_MASTER") != "unknown_section_master"
    log_item_count(df_trans.filter(mask), "POG & has sales")

    # Lookup the banner and STORE_NO
    cols = ["RETAIL_OUTLET_LOCATION_SK", "NATIONAL_BANNER_DESC", "STORE_NO"]
    df_banner_lookup = df_banner_lookup.select(*cols)
    dup_check(df_banner_lookup, ["RETAIL_OUTLET_LOCATION_SK"])

    df_trans = df_trans.join(
        other=df_banner_lookup,
        on=["RETAIL_OUTLET_LOCATION_SK"],
        how="inner",
    )

    if banner:
        log.info(f"Filtering banners to: {banner}")
        df_trans = df_trans.where(F.col("NATIONAL_BANNER_DESC").isin(*banner))
    else:
        log.info(f"Including ALL banners")

    mask = F.col("SECTION_MASTER") != "unknown_section_master"
    log_item_count(df_trans.filter(mask), "POG & has sales & in banner")

    # removes 'unknown_section_master'
    df_trans = filter_invalid_dimension_values(
        df=df_trans,
    )

    # subset to req'd cols only
    cols_reqd = [
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

    df_trans = df_trans.select(*cols_reqd)

    return df_trans


def add_customer_card_id(
    df: SparkDataFrame, df_cust_reachable: SparkDataFrame
) -> SparkDataFrame:
    """
    Add customer_id_card to any transaction type dataframe that contains customer_card_sk.

    Parameters
    ----------
    df: DataFrame
        Any DataFrame with a CUSTOMER_CARD_SK column
    df_cust_reachable:
        DataFrame that maps CUSTOMER_CARD_SK to CUSTOMER_CARD_ID
    Returns
    -------
    df: DataFrame
        Original DataFrame with CUSTOMER_CARD_ID representing a unique customer
    """

    df_flag = df_cust_reachable

    # de-dup the contactability flag data to take the latest
    # 'DROP_MODIFIED_DATE' of the file
    dims = ["CUSTOMER_CARD_SK"]
    col_date = "DROP_MODIFIED_DATE"
    agg = [F.max(col_date).alias(col_date)]
    df_latest_records = df_flag.groupBy(*dims).agg(*agg)
    df_flag = df_flag.join(df_latest_records, dims + [col_date], "inner")
    dup_check(df_flag, dims)

    # customer card sk from the dataframe that contains the mapping to customer
    # mapping has an additional 2 check number that needs to be removed to map
    # to the transaction tables
    col_cust_sk = F.substring("CUSTOMER_CARD_SK", 2, len("CUSTOMER_CARD_SK"))
    df_cust_ids = df_flag.withColumn("CUSTOMER_CARD_SK", col_cust_sk)

    # take only the lookup of "CUSTOMER_CARD_ID" by "SK"
    cols = ["CUSTOMER_CARD_SK", "CUSTOMER_CARD_ID"]
    df_cust_ids.select(*cols).dropDuplicates()

    # this represents the default value for a record in the transaction file
    # that is not linked to a customer card. We need to filter those out
    df_cust_ids = df_cust_ids.where(F.col("CUSTOMER_CARD_ID") != "-2")

    # inner join the IDs in on SK
    df = df.join(df_cust_ids, on="CUSTOMER_CARD_SK", how="inner")

    return df


def aggregate_to_cust_basket_item(df: SparkDataFrame) -> SparkDataFrame:
    """
    Aggregates (de-dups) transactional data to create a "skeleton"
    dataset containing only required dimensions:
    customer / basket / item level / POG section / region / banner

    Parameters
    ----------
    df: transactional data

    Returns
    -------
    every combination of the required dimensions (a "skeleton" dataset)
    """

    # Make data unique by customer, basket and item
    cols_dim = [
        "CUSTOMER_CARD_ID",
        "TRANSACTION_RK",
        "ITEM_NO",
        "SECTION_MASTER",
        "REGION",
        "NATIONAL_BANNER_DESC",
    ]

    df_processed = df.select(*cols_dim).dropDuplicates(subset=cols_dim)

    # validate we are on the right dimension
    # dup_check(df_processed, cols_dim)

    return df_processed


def get_frequent_visitors(df: SparkDataFrame, visit_thresh: int) -> SparkDataFrame:
    """
    Keeps only customers that have visited more than the specified threshold

    Parameters
    ----------
    df: DataFrame
      DataFrame containing CUSTOMER_CARD_ID and TRANSACTION_RK
    visit_thresh: int
      Threshold for the number of cusotmer visits

    Returns
    -------
    cust_visit_gt_thresh: DataFrame
      DataFrame containing only customers that visited more than the threshold

    """

    # Get count of visits by customer
    cust_visit_count = df.groupBy("CUSTOMER_CARD_ID").agg(
        F.countDistinct("TRANSACTION_RK").alias("NUM_VISITS")
    )

    # Get customers visiting more than the threshold
    cust_visit_gt_thresh = cust_visit_count.where(
        F.col("NUM_VISITS") > visit_thresh
    ).select("CUSTOMER_CARD_ID")

    # since 'cust_visit_gt_thresh' is a much smaller (dimensional) dataset
    # it makes sense to broadcast it across cluster which might speed up
    # the join
    cust_visit_gt_thresh = F.broadcast(cust_visit_gt_thresh)

    # Keep only those customers visiting more than the threshold
    cust_visit_gt_thresh = df.join(
        other=cust_visit_gt_thresh,
        on=["CUSTOMER_CARD_ID"],
        how="inner",
    )

    return cust_visit_gt_thresh


def remove_low_volume_items(df: SparkDataFrame, item_vol_thresh: int) -> SparkDataFrame:
    """
    Keeps only items that have been purchased by more customers than the specified threshold

    Parameters
    ----------
    df: DataFrame
      DataFrame containing CUSTOMER_CARD_ID and ITEM_NO
    item_vol_thresh: int
      Threshold for the number of customers that must have purchased an item

    Returns
    -------
    cust_visit_gt_thresh: DataFrame
      DataFrame containing only all customers and baskets excluding items purchased by fewer customers than the threshold

    """
    # Get count of number of customers purchasing each item
    agg = [F.countDistinct("CUSTOMER_CARD_ID").alias("CUSTS_PURCHASED")]
    item_vol_count = df.groupBy("ITEM_NO").agg(*agg)

    # Get items that were purchased by more customers than the specified threshold
    mask = F.col("CUSTS_PURCHASED") > item_vol_thresh
    item_visit_gt_thresh = item_vol_count.where(mask).select("ITEM_NO")

    # since 'cust_visit_gt_thresh' is a much smaller (dimensional) dataset
    # it makes sense to broadcast it across cluster which might speed up
    # the join
    item_visit_gt_thresh = F.broadcast(item_visit_gt_thresh)

    # Keep only the items that were purchased by more customers than the specified threshold
    item_visit_gt_thresh = df.join(item_visit_gt_thresh, on=["ITEM_NO"], how="inner")

    return item_visit_gt_thresh


def get_items_diff_trans(df: SparkDataFrame) -> SparkDataFrame:
    """
    Gets DataFrame at the customer level and item pair
    that contains all item pairs purchased by
    that customer in DIFFERENT transactions

    Parameters
    ----------
    df: DataFrame
      DataFrame containing CUSTOMER_CARD_ID, TRANSACTION_RK and ITEM_NO

    Returns
    -------
    combos_diff: DataFrame
      DataFrame containing all items purchased by the customer in DIFFERENT
      transactions

    """

    # Get all combinations of items and baskets bought by the same customer
    combos_diff_a = df.select(
        F.col("CUSTOMER_CARD_ID"),
        F.col("SECTION_MASTER"),
        F.col("REGION"),
        F.col("NATIONAL_BANNER_DESC"),
        F.col("TRANSACTION_RK").alias("BASK_KEY_A"),
        F.col("ITEM_NO").alias("ITEM_A"),
    )

    combos_diff_b = df.select(
        F.col("CUSTOMER_CARD_ID"),
        F.col("SECTION_MASTER"),
        F.col("REGION"),
        F.col("NATIONAL_BANNER_DESC"),
        F.col("TRANSACTION_RK").alias("BASK_KEY_B"),
        F.col("ITEM_NO").alias("ITEM_B"),
    )

    combos_diff_a = combos_diff_a.repartition("CUSTOMER_CARD_ID")
    combos_diff_b = combos_diff_b.repartition("CUSTOMER_CARD_ID")

    df_combos_diff = combos_diff_a.join(
        other=combos_diff_b,
        on=["CUSTOMER_CARD_ID", "SECTION_MASTER", "REGION", "NATIONAL_BANNER_DESC"],
        how="inner",
    )

    # TODO: Jason suggestion: get in a different dataframe list of paris that
    #  were bought in the same basket to penalize prod2vec

    # Get only records where the items were purchased in different transactions
    df_combos_diff = df_combos_diff.where(F.col("BASK_KEY_A") != F.col("BASK_KEY_B"))

    # Remove records where ITEM_A == ITEM_B
    df_combos_diff = df_combos_diff.where(F.col("ITEM_A") != F.col("ITEM_B"))

    # Keep only the unique items
    cols = [
        F.col("CUSTOMER_CARD_ID"),
        F.col("SECTION_MASTER"),
        F.col("REGION"),
        F.col("NATIONAL_BANNER_DESC"),
        F.col("ITEM_A"),
        F.col("ITEM_B"),
    ]

    df_combos_diff = df_combos_diff.select(*cols).dropDuplicates()

    return df_combos_diff


def pre_process_txnitem(
    df_txnitem: SparkDataFrame,
    st_date: str,
    end_date: str,
) -> SparkDataFrame:
    """
    Contains standard logic for txnitem pre-processing that should
    be included in ALL usages of this table
    Parameters
    ----------
    df_txnitem: raw txnitem data
    st_date: start date for which txnitem data is needed
    end_date: end date for which txnitem data is needed

    Returns
    -------
    pre-processed txnitem
    """

    df = df_txnitem.where(
        (F.col("CALENDAR_DT") >= st_date)
        & (F.col("CALENDAR_DT") <= end_date)
        & (
            F.col("CUSTOMER_SK") != 1
        )  # Keep only customer records that don't have a 1 which indicates they don't have a match
    )

    return df


def log_item_count(df: SparkDataFrame, msg: str, col_name: str = "ITEM_NO") -> None:
    n_items = df.select(col_name).dropDuplicates().count()
    log.info(f"ITEM COUNT: {n_items} ({msg})")


def lookup_section_master(
    df_txnitem: SparkDataFrame,
    pog_section_list: List[str],
    df_item_pog_section_lookup: SparkDataFrame,
    df_product: SparkDataFrame,
    df_location: SparkDataFrame,
):
    df_prod = df_product
    df_trans = df_txnitem

    # Lookup the product data and filter to specified category
    if pog_section_list:
        log.info(f"Including the following POG sections: {pog_section_list}")
        mask = F.col("SECTION_MASTER").isin(*pog_section_list)
        df_item_scope = df_item_pog_section_lookup.filter(mask)
    else:
        log.info(f"Including ALL POG sections!")
        df_item_scope = df_item_pog_section_lookup

    # pog data might have some invalid item no's - filter them out
    mask = is_col_null_mask("ITEM_NO")
    df_item_scope = df_item_scope.filter(~mask)

    # quick dup check on the POG data
    dup_check(df_item_scope, ["ITEM_NO", "STORE"])
    df_item_scope = df_item_scope.withColumnRenamed("STORE", "STORE_NO")

    # get raw item count
    log_item_count(df_item_scope, "given POG data for all region/banner")

    # get the RETAIL_OUTLET_LOCATION_SK to be able to join with txnitem
    # NOTE: this will explode and this is expected because we want ALL
    # possible RETAIL_OUTLET_LOCATION_SK's for the given store
    cols = ["STORE_NO", "RETAIL_OUTLET_LOCATION_SK"]
    df_item_scope = df_item_scope.join(df_location.select(*cols), "STORE_NO", "inner")

    # should not be unique on the SK level for location
    dup_check(df_item_scope, ["ITEM_NO", "RETAIL_OUTLET_LOCATION_SK"])
    log_item_count(df_item_scope, "after 'location' join")

    # now get the ITEM_SK to be able to join with txnitem
    # NOTE: this will explode and this is expected because we want ALL
    # possible ITEM_SK's for the given item
    cols = ["ITEM_NO", "ITEM_SK"]
    df_item_scope = df_item_scope.join(df_prod.select(*cols), "ITEM_NO", "inner")

    # should not be unique on the SK level for item
    dup_check(df_item_scope, ["ITEM_SK", "RETAIL_OUTLET_LOCATION_SK"])
    log_item_count(df_item_scope, "after 'product' join")

    # get required columns only
    cols = ["ITEM_SK", "RETAIL_OUTLET_LOCATION_SK", "SECTION_MASTER"]
    df_item_scope = df_item_scope.select(*cols)

    # lookup SECTION_MASTER in transaction data using ITEM_SK
    dims = ["ITEM_SK", "RETAIL_OUTLET_LOCATION_SK"]
    dup_check(df_item_scope, dims)
    df_trans = df_trans.join(df_item_scope, on=dims, how="left")

    # fill with 'unknown_section_master' for non-matches
    unknown_sm_val = "unknown_section_master"
    mask = F.col("SECTION_MASTER").isNull()
    col_unkn = F.lit(unknown_sm_val)
    col = F.when(mask, col_unkn).otherwise(F.col("SECTION_MASTER"))
    df_trans = df_trans.withColumn("SECTION_MASTER", col)

    # get the ITEM_NO into txnitem
    df_trans = df_trans.join(df_prod, "ITEM_SK", "inner")

    # ensure we have at least 1 match
    # TODO: in future will need require 100% match once we have a more
    #  comprehensive POG data
    df_check = df_trans.filter(F.col("SECTION_MASTER") != unknown_sm_val)
    msg = f"No matches in txnitem data for POG(s): {pog_section_list}"
    assert df_check.limit(1).count() > 0, msg
    log_item_count(df_check, "in product & POG")

    return df_trans


def log_entity_counts(df: SparkDataFrame) -> None:
    """logs entity counts in the final processed data"""

    n_cats = df.select("SECTION_MASTER").dropDuplicates().count()
    n_items_a = df.select("ITEM_A").dropDuplicates().count()
    n_items_b = df.select("ITEM_B").dropDuplicates().count()
    n_custs = df.select("CUSTOMER_CARD_ID").dropDuplicates().count()
    n_banners = df.select("NATIONAL_BANNER_DESC").dropDuplicates().count()
    n_regions = df.select("REGION").dropDuplicates().count()

    # generate a summary to print in logs
    msg = f"""
    Data was pre-processed and scoped. Here is a count summary:
    
    - Customers (CUSTOMER_CARD_ID): {n_custs}
    - Items (ITEM_A): {n_items_a}    
    - Items (ITEM_B): {n_items_b}    
    - POGs (SECTION_MASTER): {n_cats}
    - Banners (NATIONAL_BANNER_DESC): {n_banners}
    - Regions (REGION): {n_regions}
    """

    log.info(msg)

    agg = [
        F.count("*").alias("ROW_COUNT"),
        F.countDistinct("SECTION_MASTER").alias("POG_COUNT"),
        F.countDistinct("ITEM_A").alias("ITEM_COUNT"),
    ]

    pdf = df.groupBy("REGION", "NATIONAL_BANNER_DESC").agg(*agg).toPandas()
    pdf = pdf.sort_values("ROW_COUNT", ascending=False).reset_index(drop=True)

    pd.set_option("display.max_rows", None)

    msg = f"""
    Here is the summary of which region/banner combinations were produced
    as well as their corresponding counts of POGs (SECTION_MASTER)
    as well as items (ITEM_NO)
    sorted by row count from largest to lowest.
    \n{pdf}
    """

    log.info(msg)


def filter_invalid_dimension_values(df: SparkDataFrame) -> SparkDataFrame:
    """currently filters out 'unknown_section_master"""

    # filter out "unknown_section_master"
    mask = F.col("SECTION_MASTER") != "unknown_section_master"
    df = df.filter(mask)
    return df


@F.udf
def mode(x):
    if len(x) == 0:
        return None
    from collections import Counter

    return Counter(x).most_common(1)[0][0]


def get_items_lost_after_explosion(
    df_low_vol: SparkDataFrame,
    df_cust_item_diff: SparkDataFrame,
):
    """
    Calculates a summary of items that were lost during the explosion
    where we filter for:
        BASK_KEY_A!=BASK_KEY_B
        ITEM_A!=ITEM_B

    Used for traceability purposes

    Parameters
    ----------
    df_low_vol: data before explosion and filtering
    df_cust_item_diff: data after the explosion and filtering

    Returns
    -------
    summary by region/banner/SM which items were dropped
    """

    cols_entities_start = [
        "REGION",
        "NATIONAL_BANNER_DESC",
        "SECTION_MASTER",
        "ITEM_NO",
    ]

    df_1 = df_low_vol.select(*cols_entities_start).dropDuplicates()

    cols_entities_end = [
        "REGION",
        "NATIONAL_BANNER_DESC",
        "SECTION_MASTER",
    ]

    cols_entities_1 = cols_entities_end + [F.col("ITEM_A").alias("ITEM_NO")]
    cols_entities_2 = cols_entities_end + [F.col("ITEM_B").alias("ITEM_NO")]

    df_2 = union_dfs(
        [
            df_cust_item_diff.select(*cols_entities_1).dropDuplicates(),
            df_cust_item_diff.select(*cols_entities_2).dropDuplicates(),
        ]
    ).dropDuplicates()

    agg = [
        F.countDistinct(F.col("ITEM_NO")).alias("ITEM_COUNT_1"),
        F.collect_set(F.col("ITEM_NO")).alias("ITEM_LIST_1"),
    ]

    df_1_s = df_1.groupBy(*cols_entities_end).agg(*agg)

    agg = [
        F.countDistinct(F.col("ITEM_NO")).alias("ITEM_COUNT_2"),
        F.collect_set(F.col("ITEM_NO")).alias("ITEM_LIST_2"),
    ]

    df_2_s = df_2.groupBy(*cols_entities_end).agg(*agg)

    # perform various calculations to get: item count that was lost,
    # item ratio and percentage that was lost, etc etc
    col_items_count_lost = F.col("ITEM_COUNT_1") - F.col("ITEM_COUNT_2")
    col_ratio_lost = F.col("ITEM_COUNT_LOST") / F.col("ITEM_COUNT_1")
    col_item_perc_lost = F.round(100 * (col_ratio_lost), 1)
    col_item_list_lost = F.array_except(F.col("ITEM_LIST_1"), F.col("ITEM_LIST_2"))
    cols_meas = ["ITEM_COUNT_1", "ITEM_COUNT_LOST", "ITEM_PERC_LOST", "ITEM_LIST_LOST"]

    # create a table that contains the above summary calculations to be able
    # to store than and review later
    df_all = (
        df_1_s.join(df_2_s, cols_entities_end, "inner")
        .withColumn("ITEM_COUNT_LOST", col_items_count_lost)
        .withColumn("ITEM_PERC_LOST", col_item_perc_lost)
        .withColumn("ITEM_LIST_LOST", col_item_list_lost)
        .select(*(cols_entities_end + cols_meas))
    )

    # do a higher-level summary on region/banner to be able to display in logs
    agg = [
        F.sum("ITEM_COUNT_1").alias("ITEM_COUNT_1"),
        F.sum("ITEM_COUNT_LOST").alias("ITEM_COUNT_LOST"),
    ]

    df_reg_ban = (
        df_all.groupBy("REGION", "NATIONAL_BANNER_DESC")
        .agg(*agg)
        .withColumn("ITEM_PERC_LOST", col_item_perc_lost)
        .orderBy("REGION", "NATIONAL_BANNER_DESC")
    )

    pdf_reg_ban = df_reg_ban.toPandas()
    pd.set_option("display.max_rows", None)
    log.info(f"Items lost after explosion:\n{pdf_reg_ban}\n")

    df_lost = df_all.filter(F.col("ITEM_COUNT_LOST") > 0)
    df_lost = df_lost.orderBy(F.col("ITEM_COUNT_LOST").desc())
    return df_lost


def validate_using_exec_id(df: SparkDataFrame) -> None:
    """
    This is the ultimate check that ensures that the SECTION_MASTER
    cleaning and deduping logic is correct. This check ensures that
    when we generate our Execution ID (a unit of run for the model)
    it should generate a 1-to-1 mapping, lets check it
    first create dummy region/banner as those are irrelevant here but
    are needed to generate Execution ID.
    if this check does not pass, this will cause downstream models to
    use wrong EXEC_ID's and therefore wrong data that come with them
    """

    df = df.drop("REGION").drop("NATIONAL_BANNER_DESC")

    # first create dummy region banner
    cols_check = [F.lit("r").alias("REGION"), F.lit("b").alias("NATIONAL_BANNER_DESC")]
    df_check = df.select("*", *cols_check)

    # generate the Execution ID
    df_check = generate_execution_id(df=df_check)
    # if the SECTION_MASTER's are consistent than if we drop duplicates
    # on all 4 dimensions the data would still be unique on Execution ID
    dims = ["EXEC_ID", "REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER"]
    df_check = df_check.select(*dims).dropDuplicates()

    msg = f"""
    Dup check failed in 'validate_using_exec_id'!
    Check that POG processing and cleaning logic is correct.
    This is the ultimate check that ensures that the SECTION_MASTER
    cleaning and de-duping logic is correct. This check ensures that 
    when we generate our Execution ID (a unit of run for the model)
    it should generate a 1-to-1 mapping, lets check it
    first create dummy region/banner as those are irrelevant here but
    are needed to generate Execution ID.
    if this check does not pass, this will cause downstream models to 
    use wrong EXEC_ID's and therefore wrong data that come with them    
    """

    # PLEASE DON'T DISABLE THIS DUP CHECK! read above why it is important
    dup_check(df=df_check, dims=["EXEC_ID"], assert_=True, msg=msg)


class ItemLostInNSReason(str, enum.Enum):
    """ various reasons why an item can be lost in NS """

    # this reason is used when we filter down data to only valid customer
    # card IDs, i.e. non -2, i.e. those with loyalty profile
    ONLY_VALID_CUSTOMER_CARD = "ONLY_VALID_CUSTOMER_CARD"

    # this reason is used when we filter for >x visits visit threshold
    # per customer. I.e. a customer must visit ANY store >x number of times
    # to be included
    VISIT_THRESH = "VISIT_THRESH"

    # this reason is used when we filter for >x item volume threshold
    # the item must be purchased by at least x number of customers
    VOL_THRESH = "VOL_THRESH"

    # this reason is used when we explode the items for each customer into
    # item "pairs" that were never bought together by that customer
    PAIRS_CREATION = "PAIRS_CREATION"

    def __str__(self):
        return self.value


def capture_items_lost(
    df_start: SparkDataFrame,
    df_end: SparkDataFrame,
    reason: ItemLostInNSReason,
) -> SparkDataFrame:
    """
    Compares the "before" (df_start) and "after" (df_end) data to obtain the
    difference in scope (region/banner/SM/item).
    We store this information to be able to flag which items were lost during
    each step in NS pre-processing module.

    Parameters
    ----------
    df_start : SparkDataFrame
        starting data to compare to

    df_end : SparkDataFrame
        resulting data to evaluate

    reason : ItemLostInNSReason
        reason for losing item

    Returns
    -------
    df containing list of region/banner/SM/item along with the reason for
    dropping items
    """

    # set the required dimensions
    dims = ["REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER", "ITEM_NO"]

    # check that the 'start' DF has the required dimensions
    msg = f"'df_start' must have all of these dimensions: {dims}"
    assert len(set(dims) - set(df_start.columns)) == 0, msg

    # # check that the 'end' DF has the required dimensions
    msg = f"'df_end' must have all of these dimensions: {dims}"
    assert len(set(dims) - set(df_end.columns)) == 0, msg

    # get the start/end scope to be able to compare
    df_start_scope = df_start.select(*dims).dropDuplicates()
    df_end_scope = df_end.select(*dims).dropDuplicates()

    # get only the scope that is in the 'start' but not in the 'end'
    # to get the 'lost' slice
    df_lost = df_start_scope.join(df_end_scope, dims, "left_anti")

    # assign the reason why it was lost
    df_lost = df_lost.withColumn("ITEM_LOST_IN_NS_REASON", F.lit(str(reason)))

    return df_lost


def get_distinct_region_banner_item_from_pairs(df: SparkDataFrame) -> SparkDataFrame:
    """
    obtains distinct set of REGION / BANNER / SECTION_MASTER / ITEM_A / ITEM_B
    from a given 'df'.
    Used to be able to learn which items are present in each EXEC_ID when
    there is no ITEM_NO column available, and only 'pairs' of items

    Parameters
    ----------
    df : SparkDataFrame
        a dataset with region / banner / dept and item pairs

    Returns
    -------
    a dataset with distinct set of items for each region / banner / dept
    """

    # set the required dimensions
    dims_main = ["REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER"]
    dims = dims_main + ["ITEM_A", "ITEM_B"]

    # check that the input DF has the required dimensions
    msg = f"'df' must have all of these dimensions: {dims}"
    assert len(set(dims) - set(df.columns)) == 0, msg

    # stack on top of each item_a and item_b
    cols = dims_main + [F.col("ITEM_A").alias("ITEM_NO")]
    df_1 = df.select(*cols)
    cols = dims_main + [F.col("ITEM_B").alias("ITEM_NO")]
    df_2 = df.select(*cols)

    df_result = union_dfs([df_1, df_2], align_schemas=True).dropDuplicates()

    return df_result


def combine_items_lost(
    df_items_lost_only_valid_customer_card: SparkDataFrame,
    df_items_lost_visit_thresh: SparkDataFrame,
    df_items_lost_vol_thresh: SparkDataFrame,
    df_items_lost_pairs_creation: SparkDataFrame,
):
    """
    Combines together the slices of region/banner/SM/item that were lost
    during the NS pre-proc process

    Parameters
    ----------
    df_items_lost_only_valid_customer_card : SparkDataFrame
        set of entities lost when we filter down data to only valid customer
        card IDs, i.e. non -2, i.e. those with loyalty profile

    df_items_lost_visit_thresh : SparkDataFrame
        set of entities lost when we filter for >x visits visit threshold
        per customer. I.e. a customer must visit ANY store >x number of times
        to be included

    df_items_lost_vol_thresh : SparkDataFrame
        set of entities lost when we filter for >x item volume threshold
        the item must be purchased by at least x number of customers

    df_items_lost_pairs_creation : SparkDataFrame
        set of entities lost when we explode the items for each customer into
        item "pairs" that were never bought together by that customer

    Returns
    -------
    combined set of entities lost along with a loss reason
    """

    # set the required dimensions
    dims = ["REGION", "NATIONAL_BANNER_DESC", "SECTION_MASTER", "ITEM_NO"]

    # create list of DFs
    list_dfs = [
        df_items_lost_only_valid_customer_card,
        df_items_lost_visit_thresh,
        df_items_lost_vol_thresh,
        df_items_lost_pairs_creation,
    ]

    # check that the input DF has the required dimensions
    msg = f"all DFs passed must have all of these dimensions: {dims}"
    assert all([len(set(dims) - set(x.columns)) == 0 for x in list_dfs]), msg

    # combine together
    df = union_dfs(list_dfs, align_schemas=True)

    return df


def add_case_pack_info(
    df_pog: SparkDataFrame,
    df_apollo: SparkDataFrame,
) -> SparkDataFrame:
    """
    Looks up 'CASEPACK' information into the overall merged POG (df_pog) from
    Apollo. This is not ideal as there are items that are unique to
    Spaceman, and for those we won't be able to lookup CASEPACK.
    The overlap between Apollo and Spaceman in terms of items is only ~20%
    with the current version of files (2022-04-06)

    Parameters
    ----------
    df_pog : SparkDataFrame
        overall merged POG data

    df_apollo : SparkDataFrame
        processed apollo data

    Returns
    -------
    overall merged POG data with 'CASEPACK' information
    """

    # first ensure that CASEPACK is completely populate
    # TODO: ideally should be part of the data contract and validated
    #  automatically on read
    msg = f"Some 'CASEPACK' information is missing from Apollo"
    assert df_apollo.filter(is_col_null_mask("CASEPACK")).limit(1).count() == 0, msg

    # for Apollo slice we are going to do the proper store/item lookup
    # create a lookup table on 'ITEM_NO'/'STORE' level for 'CASEPACK'
    # for Apollo
    dims = ["STORE", "ITEM_NO"]
    dup_check(df_apollo, dims)
    cols = dims + [F.col("CASEPACK").alias("CASE_PACK_APOLLO")]
    df_lookup_apollo = df_apollo.select(*cols)
    df_pog = df_pog.join(df_lookup_apollo, dims, "left")

    # for Spaceman slice we are going to take the most frequent 'CASEPACK'
    # across stores (i.e. the mode)
    df_counts = df_apollo.groupBy("ITEM_NO", "CASEPACK").count()
    window = Window.partitionBy("ITEM_NO").orderBy(F.desc("count"))
    col = F.row_number().over(window)
    df_counts = df_counts.withColumn("rank", col)
    mask = F.col("rank") == 1
    cols = ["ITEM_NO", F.col("CASEPACK").alias("CASE_PACK_SPACEMAN")]
    df_lookup_spaceman = df_counts.filter(mask).select(*cols)
    dup_check(df_lookup_spaceman, ["ITEM_NO"])
    df_pog = df_pog.join(df_lookup_spaceman, ["ITEM_NO"], "left")

    # assign the correct version of looked up "CASEPACK" depending on the
    # "SOURCE"
    mask_apollo = F.col("SOURCE") == "APOLLO"
    mask_spaceman = F.col("SOURCE") == "SPACEMAN"

    col_case_pack = (
        F.when(mask_apollo, F.col("CASE_PACK_APOLLO"))
        .when(mask_spaceman, F.col("CASE_PACK_SPACEMAN"))
        .otherwise(F.lit("unknown"))
    )

    df_pog = df_pog.withColumn("CASEPACK", col_case_pack)

    # check that we don't have 'unknown' 'CASEPACK' values
    df_pog.persist()
    mask = F.col("CASEPACK") == "unknown"
    msg = "'CASEPACK' lookup isn't clean, please review above code logic"
    assert df_pog.filter(mask).limit(1).count() == 0, msg

    # check for non-match %
    mask = is_col_null_mask("CASEPACK")
    n_tot_items = df_pog.select("ITEM_NO").dropDuplicates().count()
    n_bad_items = df_pog.filter(mask).select("ITEM_NO").dropDuplicates().count()
    per_bad_items = round(100 * (n_bad_items / n_tot_items), 1)
    msg = f"Percentage of items without 'CASEPACK': {per_bad_items}%"
    log.info(msg)
    df_pog.unpersist()

    return df_pog


def add_product_info(
    df_pog: SparkDataFrame, df_product: SparkDataFrame
) -> SparkDataFrame:
    """
    Looks up 'CASEPACK' information into the overall merged POG (df_pog) from
    Apollo. This is not ideal as there are items that are unique to
    Spaceman, and for those we won't be able to lookup CASEPACK.
    The overlap between Apollo and Spaceman in terms of items is only ~20%
    with the current version of files (2022-04-06)

    Parameters
    ----------
    df_pog : SparkDataFrame
        overall merged POG data

    df_product : SparkDataFrame
        product table (external)

    Returns
    -------
    overall merged POG data with 'CASEPACK' information
    """

    # shortcuts
    df_prd = df_product
    df_pog = df_pog

    # pre-process product table
    df_prod = dedup_product(df_prd, ["ITEM_NO"])
    cols = ["ITEM_NO", "UPC_NO", "ITEM_UOM_CD"]
    df_prod = df_prod.select(*cols)

    # join in the required UPC + UOM information
    df_pog = df_pog.join(df_prod, ["ITEM_NO"], "left")

    # check for non-match %
    df_pog.persist()
    df_check = filter_df_to_invalid(df_pog, ["UPC_NO", "ITEM_UOM_CD"])
    n_tot_items = df_pog.select("ITEM_NO").dropDuplicates().count()
    n_bad_items = df_check.select("ITEM_NO").dropDuplicates().count()
    per_bad_items = round(100 * (n_bad_items / n_tot_items), 1)
    msg = f"Portion of items without either UPC or UOM info: {per_bad_items}%"
    log.info(msg)
    df_pog.unpersist()

    return df_pog
