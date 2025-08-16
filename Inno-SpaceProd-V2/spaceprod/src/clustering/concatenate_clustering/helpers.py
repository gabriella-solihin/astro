from inno_utils.loggers import log
from pyspark.sql import SparkSession
from spaceprod.utils.data_transformation import check_invalid
from spaceprod.utils.imports import SparkDataFrame
from spaceprod.utils.validation import dup_check, dup_check_pd


def concatenate_clusters(
    spark: SparkSession,
    df_internal_clusters: SparkDataFrame,
    df_external_clusters: SparkDataFrame,
) -> SparkDataFrame:
    """helper to merge newly created external clusters with the internal clusters

    Parameters
    ----------
    spark: SparkSession,
        regular spark session
    df_internal_clusters : SparkDataFrame
        internal clustering results
    df_external_clusters: SparkDataFrame
        external clustering results
    Returns
    -------
    SparkDataFrame
        stores with merged clusters as a column
    """

    pdf_int = df_internal_clusters.toPandas()
    pdf_ext = df_external_clusters.toPandas()

    ###########################################################################
    # PERFORM AD-HOC RENAMING AND TYPE CONVERSION
    # TODO: All of this should not happen in the code ideally
    ###########################################################################

    pdf_int = pdf_int.rename(
        columns={
            "store_cluster": "INTERNAL_CLUSTER",
            "REGION": "REGION",
            "BANNER": "BANNER",
            "STORE_PHYSICAL_LOCATION_NO": "STORE_PHYSICAL_LOCATION_NO",
        },
        errors="raise",
    )

    pdf_ext = pdf_ext.rename(
        columns={
            "Cluster_Labels": "EXTERNAL_CLUSTER",
            "Region_Desc": "REGION",
            "Banner": "BANNER",
            "Store_Physical_Location_No": "STORE_PHYSICAL_LOCATION_NO",
            "Banner_Key": "BANNER_KEY",
        },
        errors="raise",
    )

    # perform require type conversions for consistency
    pdf_ext["EXTERNAL_CLUSTER"] = pdf_ext["EXTERNAL_CLUSTER"].astype(int).astype(str)
    pdf_int["INTERNAL_CLUSTER"] = pdf_int["INTERNAL_CLUSTER"].astype(int).astype(str)

    col_store = pdf_ext["STORE_PHYSICAL_LOCATION_NO"].astype(int).astype(str)
    pdf_ext["STORE_PHYSICAL_LOCATION_NO"] = col_store

    col_store = pdf_int["STORE_PHYSICAL_LOCATION_NO"].astype(int).astype(str)
    pdf_int["STORE_PHYSICAL_LOCATION_NO"] = col_store

    # value patching and conversion
    # consistent case, etc
    pdf_int["REGION"] = pdf_int["REGION"].str.lower().str.strip()
    pdf_ext["REGION"] = pdf_ext["REGION"].str.lower().str.strip()
    pdf_int["BANNER"] = pdf_int["BANNER"].str.upper().str.strip()
    pdf_ext["BANNER"] = pdf_ext["BANNER"].str.upper().str.strip()

    ###########################################################################
    # CHANGE EXTERNAL CLUSTERS TO BE LETTERS, INSTEAD OF NUMBERS
    ###########################################################################

    replacements = {
        str(index + 1): letter
        for index, letter in enumerate("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    }

    pdf_ext["EXTERNAL_CLUSTER"] = pdf_ext["EXTERNAL_CLUSTER"].replace(replacements)

    ###########################################################################
    # JOIN IN THE INTERNAL AND EXTERNAL CLUSTERING
    ###########################################################################

    # first do necessary dup checks to ensure we have the right dimensionality
    dims = ["STORE_PHYSICAL_LOCATION_NO", "REGION", "BANNER"]
    dup_check_pd(pdf_int, dims)
    dup_check_pd(pdf_ext, dims)

    # next ensure we won't create duplicate columns after the join
    cols_other_int = [x for x in pdf_int.columns if x not in dims]
    cols_other_ext = [x for x in pdf_ext.columns if x not in dims]

    cols_inter = list(set(cols_other_int).intersection(set(cols_other_ext)))
    msg = f"The following cols are common between EXT and INT clustering: {cols_inter}"
    assert len(cols_inter) == 0, msg

    # perform the join
    pdf_merged_clusters = pdf_ext.merge(
        right=pdf_int,
        on=dims,
        how="inner",
        suffixes=(None, None),
    )

    # finally check to ensure the join was successful and we did not

    msg = f"No matches between environics data and ext.clustering on: {dims}"
    assert len(pdf_merged_clusters) > 0, msg

    # check which entities were lost
    # TODO: in future we might want to break here if we are losing any of the
    #  entities, i.e.: banner keys / regions / banners
    in_ext = pdf_ext.set_index(dims).index.difference(pdf_int.set_index(dims).index)
    in_int = pdf_int.set_index(dims).index.difference(pdf_ext.set_index(dims).index)

    msg_lost = f"""
    ATTENTION!
    The following banner / region / banner key combinations are ONLY 
    found in ether EXTERNAL or INTERNAL clustering,
    and therefore they are ***LOST*** during the "merge cluster" task:
    
     - Only in EXTERNAL ({len(in_ext)}): {in_ext.to_list()}
     - Only in INTERNAL ({len(in_int)}): {in_int.to_list()}
     
    Common combination count: {len(pdf_merged_clusters)} (preserved)
    """

    msg_not_lost = f"""
    None of the banner / region / banner key combinations lost
    Total number of combiantions: {len(pdf_merged_clusters)} 
    """

    msg = msg_lost if (len(in_ext) + len(in_int)) > 0 else msg_not_lost
    log.info(msg)

    ###########################################################################
    # CREATE A COMBINED CLUSTER LABEL
    ###########################################################################

    pdf_merged_clusters["MERGED_CLUSTER"] = (
        pdf_merged_clusters["INTERNAL_CLUSTER"]
        + pdf_merged_clusters["EXTERNAL_CLUSTER"]
    )

    ###########################################################################
    # PREPARE OUTPUT
    ###########################################################################

    # keep only relevant columns
    pdf_merged_clusters = pdf_merged_clusters[
        [
            "REGION",
            "BANNER",
            "BANNER_KEY",
            "STORE_PHYSICAL_LOCATION_NO",
            "INTERNAL_CLUSTER",
            "EXTERNAL_CLUSTER",
            "MERGED_CLUSTER",
        ]
    ]

    # convert back to pyspark
    df_merged_clusters = spark.createDataFrame(pdf_merged_clusters)

    return df_merged_clusters


def validate_merged_clusters(df: SparkDataFrame) -> None:
    """Performs final validity checks on merged clusters dataset"""

    # dup check
    dims = ["STORE_PHYSICAL_LOCATION_NO", "REGION", "BANNER"]
    dup_check(df, dims)

    # invalid values check
    cols_to_check = [
        "REGION",
        "BANNER",
        "BANNER_KEY",
        "STORE_PHYSICAL_LOCATION_NO",
        "INTERNAL_CLUSTER",
        "EXTERNAL_CLUSTER",
        "MERGED_CLUSTER",
    ]

    check_invalid(df, cols_to_check)
