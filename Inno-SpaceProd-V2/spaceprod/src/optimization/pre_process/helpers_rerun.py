import pandas as pd
from spaceprod.utils.data_helpers import strip_except_alpha_num
from spaceprod.utils.imports import SparkDataFrame
from spaceprod.utils.names import get_col_names


def read_sections_for_margin_reruns(
    df_region_banner_sections_for_margin_reruns: SparkDataFrame,
    df_region_banner_clusters_for_margin_reruns: SparkDataFrame,
) -> pd.DataFrame:
    """This function reads a selection of section masters per region banner in that should be optimized for margin.

    Parameters
    ----------
    spark: SparkSession
        spark session
    cfg: Dict
        optimization config

    Returns
    -------
    sections_for_reruns: pd.DataFrame
        this dataframe has a list of sections that should be optimized for margin
    """
    n = get_col_names()

    # convert to pandas for compatibility
    sections_for_reruns = df_region_banner_sections_for_margin_reruns.toPandas()
    clusters_for_margin_reruns = df_region_banner_clusters_for_margin_reruns.toPandas()

    sections_for_reruns = sections_for_reruns.rename(
        {
            "Region": n.F_REGION_DESC,
            "Banner": n.F_BANNER,
            "Department": n.F_DEPARTMENT,
            "SECTION_MASTER": n.F_SECTION_MASTER,
            "(Role = Credible or Trigger, If TRUE -> Rerun if cluster is non-price sensitive)": "MARGIN_RERUN",
        },
        axis=1,
        errors="raise",
    )

    sections_for_reruns[n.F_REGION_DESC] = sections_for_reruns[
        n.F_REGION_DESC
    ].str.lower()

    sections_for_reruns[n.F_BANNER] = sections_for_reruns[n.F_BANNER].str.upper()
    sections_for_reruns = sections_for_reruns.loc[
        sections_for_reruns["MARGIN_RERUN"] == "TRUE"
    ]
    sections_for_reruns = sections_for_reruns[
        [n.F_REGION_DESC, n.F_BANNER, n.F_DEPARTMENT, n.F_SECTION_MASTER]
    ]

    clusters_for_margin_reruns = clusters_for_margin_reruns.rename(
        {
            "BANNER": n.F_BANNER,
            "REGION": n.F_REGION_DESC,
            "MERGE_CLUSTER": n.F_M_CLUSTER,
        },
        axis=1,
        errors="raise",
    )

    clusters_for_margin_reruns[n.F_REGION_DESC] = clusters_for_margin_reruns[
        n.F_REGION_DESC
    ].str.lower()

    clusters_for_margin_reruns[n.F_BANNER] = clusters_for_margin_reruns[
        n.F_BANNER
    ].str.upper()

    sections_for_reruns = sections_for_reruns.merge(
        clusters_for_margin_reruns, on=[n.F_REGION_DESC, n.F_BANNER], how="inner"
    )

    return sections_for_reruns


def determine_margin_space_to_optimize(
    df_summary_per_store_cat_sales: SparkDataFrame,
    df_region_banner_clusters_for_margin_reruns: SparkDataFrame,
    df_region_banner_sections_for_margin_reruns: SparkDataFrame,
    df_summary_legal_breaks: SparkDataFrame,
):
    """This function determines the total section length sum for margin rerun based sections
    which is then used as upper bound department space in margin-only reruns

    Parameters
    ----------
    spark: SparkSession
        spark session
    cfg: Dict
        optimization config
    n: names dict
    df_summary_legal_breaks: SparkDataFrame
        for local space so we don't double subtract it in reruns

    Returns
    -------
    reg_ban_dept_rerun_tuple: Tuple
        this tuple is the region banner department combination which will require reruns for select sections
    """

    n = get_col_names()

    section_results = df_summary_per_store_cat_sales.toPandas()

    section_results.rename(
        columns=dict(
            zip(
                section_results.columns,
                [strip_except_alpha_num(x) for x in section_results.columns],
            )
        ),
        inplace=True,
    )

    section_results = section_results[n.F_SECTION_RESULTS_COL_FOR_MARGIN_RERUN]

    # read in margin rerun file for reference which clusters should be rerun
    margin_rerun_sections = read_sections_for_margin_reruns(
        df_region_banner_sections_for_margin_reruns=df_region_banner_sections_for_margin_reruns,
        df_region_banner_clusters_for_margin_reruns=df_region_banner_clusters_for_margin_reruns,
    )

    pdf_list_of_reruns = section_results.merge(
        margin_rerun_sections,
        on=[
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_DEPARTMENT,
            n.F_SECTION_MASTER,
            n.F_M_CLUSTER,
        ],
        how="inner",
    )

    assert len(pdf_list_of_reruns) > 1, "list of margin reruns is empty"

    pdf_list_of_reruns[n.F_OPT_WIDTH_X_FAC] = pdf_list_of_reruns[
        n.F_OPT_WIDTH_X_FAC
    ].astype(float)

    # remove duplicates based on duplciates in manual input data (e.g. seciton or cluster named twice)
    pdf_list_of_reruns = pdf_list_of_reruns.drop_duplicates(
        subset=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER]
    )

    # add local reserved space to opt width so that we don't subtract it twice in margin reruns
    local_space = df_summary_legal_breaks.toPandas()
    local_space.rename(
        columns=dict(
            zip(
                local_space.columns,
                [strip_except_alpha_num(x) for x in local_space.columns],
            )
        ),
        inplace=True,
    )
    pdf_list_of_reruns = pdf_list_of_reruns.merge(
        local_space[[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER, n.F_LOCAL_ITEM_WIDTH]],
        on=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER],
        how="inner",
    )
    # now we add the local reserved space to opt width x facings so that this is the new baseline for margin rerun
    pdf_list_of_reruns[n.F_LOCAL_ITEM_WIDTH] = pdf_list_of_reruns[
        n.F_LOCAL_ITEM_WIDTH
    ].astype(float)
    pdf_list_of_reruns[n.F_OPT_WIDTH_X_FAC] = (
        pdf_list_of_reruns[n.F_OPT_WIDTH_X_FAC]
        + pdf_list_of_reruns[n.F_LOCAL_ITEM_WIDTH]
    )
    pdf_list_of_reruns = pdf_list_of_reruns.drop([n.F_LOCAL_ITEM_WIDTH], axis=1)

    return pdf_list_of_reruns


def filter_data_preparation_files_by_rerun_sections(
    n, region, banner, dept, list_of_reruns, elasticity_df, shelve_df, cluster_info
):
    """This function filters necessary dataframes for margin reruns for a speific region banner dept

    Parameters
    ----------
    n: names dict
    region: str
        the region over which we create sets
    banner: str
        the banner over which we create sets
    dept: str
        deptartment to filter on
    list_of_reruns: pd.DataFrame
        list of reruns
    elasticity_df: pd.DataFrame
        elasticity df for sales or margin
    shelve_df: pd.DataFrame
        POG
    cluster_info: pd.DataFrame
        cluster dataframe to assign stores to clusters

    Returns
    -------
    elasticity_df: pd.DataFrame
        fitlered elasticity df for sales or margin
    shelve_df: pd.DataFrame
        fitlered POG
    cluster_info: pd.DataFrame
        filtered cluster dataframe to assign stores to clusters
    list_of_reruns: pd.DataFrame
        fitlered list of reruns
    """

    # first filter to specific reg, ban, dept from global rerun list
    list_of_reruns = list_of_reruns.loc[list_of_reruns[n.F_REGION_DESC] == region]
    list_of_reruns = list_of_reruns.loc[list_of_reruns[n.F_BANNER] == banner]
    list_of_reruns = list_of_reruns.loc[list_of_reruns[n.F_DEPARTMENT] == dept]

    # filter out all other sections in elasticity file not relevant for margin rerun
    elasticity_df = elasticity_df.merge(
        list_of_reruns[[n.F_M_CLUSTER, n.F_SECTION_MASTER]],
        on=[n.F_M_CLUSTER, n.F_SECTION_MASTER],
        how="inner",
    )

    keys = [n.F_STORE_PHYS_NO, n.F_SECTION_MASTER]
    # shelve_df = shelve_df.merge(list_of_reruns[keys], on=keys, how="inner")
    shelve_df = shelve_df  # testing whether if we don't filter we keep newly assorted items from other stores as intentioned
    # this issue only exists when we run on a subset of stores in a cluster that don't have the newly assorted items. should work now

    cluster_info = cluster_info.merge(
        list_of_reruns[[n.F_STORE_PHYS_NO, n.F_M_CLUSTER]],
        on=[n.F_STORE_PHYS_NO, n.F_M_CLUSTER],
        how="inner",
    )
    cluster_info = cluster_info.drop_duplicates(
        subset=[n.F_STORE_PHYS_NO, n.F_M_CLUSTER]
    )

    return elasticity_df, shelve_df, cluster_info, list_of_reruns
