from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

import box
from inno_utils.loggers import log
from spaceprod.src.optimization.data_objects import (
    DataContainer,
    DataObjectScopeGeneral,
    DataObjectScopeSpecific,
)
from spaceprod.src.optimization.helpers import get_opt_error_mask
from spaceprod.src.optimization.post_process.data_objects import (
    DataModellingConcatenatedResults,
    DataModellingConcatenatedResultsRerun,
)
from spaceprod.utils.data_helpers import backup_on_blob
from spaceprod.utils.imports import F, SparkDataFrame, T
from spaceprod.utils.names import ColumnNames, get_col_names


def generate_region_banner_dept_tuples_from_spark_df(
    df: SparkDataFrame,
) -> List[Tuple[str, str, str]]:
    """
    Generates the region/banner/department list of tuples from a dataframe
    Used to iterate through these combinations.

    Parameters
    ----------
    df : SparkDataFrame
        any dataset containing region / banner / department information

    Returns
    -------
    list of list of tuples, i.e.
    [
        (region 1, banner 1, department 1, ),
        (region 2, banner 2, department 2, ),
        ...
        (region N, banner N, department N, ),
    ]
    """

    cols = [
        "REGION",
        "BANNER",
        "DEPT",
    ]

    pdf = df.select(*cols).dropDuplicates().toPandas()

    list_dict = pdf.to_dict("records")

    region_banner_dept_tuple = [
        (
            x["REGION"],
            x["BANNER"],
            x["DEPT"],
        )
        for x in list_dict
    ]

    return region_banner_dept_tuple


def concatenate_and_summarize_all_results(
    path_opt_data_containers: str,
    region_banner_dept_tuple: tuple,
    dependent_var: str,
    rerun: bool,
):
    """This function reads all individual region banner department results from results processing and concatenates them.

    Blob outputs as CSVs of results

    Parameters
    ----------
    dependent_var: str
        passes Sales or Margin through - determines which curves to us
    """

    n = get_col_names()

    # initialize lists of results
    item_output_list = []
    summary_per_store_cat_list = []
    summary_facing_changes_list = []
    summary_legal_breaks_list = []
    supplier_space_analysis_list = []

    for region, banner, dept in region_banner_dept_tuple:
        # region, banner, dept = "quebec", "IGA", "Impulse"

        reg_ban_dept_str = f"{region}-{banner}-{dept}"
        # read in all data
        log.info(f"Reading results for concatenation for {reg_ban_dept_str}")

        scope = DataObjectScopeSpecific(
            region=region,
            banner=banner,
            dept=dept,
        )

        data_container = DataContainer(scope=scope)

        name_data_object = (
            "DataModellingResultsProcessingPerRegBanDeptRerun"
            if rerun
            else "DataModellingResultsProcessingPerRegBanDept"
        )

        data_input = data_container.read(
            path_parent=path_opt_data_containers,
            container_name=name_data_object,
        )

        item_output = data_input.item_output
        summary_per_store_cat = data_input.summary_per_store_and_cat
        summary_facing_changes = data_input.summary_facing_changes
        summary_legal_breaks = data_input.summary_legal_breaks
        supplier_space_analysis = data_input.supplier_space_analysis

        item_output_list.append(item_output)
        summary_per_store_cat_list.append(summary_per_store_cat)
        summary_facing_changes_list.append(summary_facing_changes)
        summary_legal_breaks_list.append(summary_legal_breaks)
        supplier_space_analysis_list.append(supplier_space_analysis)

    item_output_master = pd.concat(item_output_list, ignore_index=True)
    summary_per_store_master = pd.concat(summary_per_store_cat_list, ignore_index=True)
    summary_facing_master = pd.concat(summary_facing_changes_list, ignore_index=True)
    summary_legal_breaks_master = pd.concat(
        summary_legal_breaks_list, ignore_index=True
    )
    supplier_space_analysis_master = pd.concat(
        supplier_space_analysis_list, ignore_index=True
    )

    # TODO drop cluster in the aggregation summary dfs below since they will be different clusters across reg/ban once we have more?
    quality_assurance_check(item_output_master)

    (
        region_banner_dept_store_summary,
        region_banner_dept_summary,
        region_banner_summary,
    ) = calculate_region_banner_dept_store_summary(n, dependent_var, item_output_master)

    # OLD PATHS:
    # item_output_master -> f"{final_outputs_location}item_output_{dependent_var}.csv
    # summary_per_store_master ->f"{final_outputs_location}summary_per_store_cat_{dependent_var}.csv"
    # summary_facing_master ->f"{final_outputs_location}summary_facing_changes_{dependent_var}.csv"
    # region_banner_dept_store_summary ->f"{final_outputs_location}region_banner_dept_store_summary_{dependent_var}.csv"
    # region_banner_dept_summary ->f"{final_outputs_location}region_banner_dept_summary_{dependent_var}.csv"
    # region_banner_summary ->f"{final_outputs_location}region_banner_summary_{dependent_var}.csv"

    DataObject = (
        DataModellingConcatenatedResultsRerun
        if rerun
        else DataModellingConcatenatedResults
    )

    data_modelling_concatenated_results = DataObject(
        item_output_master=item_output_master,
        summary_per_store_master=summary_per_store_master,
        summary_facing_master=summary_facing_master,
        summary_legal_breaks_master=summary_legal_breaks_master,
        supplier_space_analysis_summary=supplier_space_analysis_master,
        region_banner_dept_store_summary=region_banner_dept_store_summary,
        region_banner_dept_summary=region_banner_dept_summary,
        region_banner_summary=region_banner_summary,
        scope=DataObjectScopeGeneral(),
    )

    return data_modelling_concatenated_results


def prep_optimized_output_df(x_df: pd.DataFrame, col: str) -> pd.DataFrame:
    """This function filter for only optimized assignments and preps data

    Parameters
    ----------
    x_prep_df: pd.DataFrame
        this is the dataframe that has the optmization result per item (how many facings)
    col: str
        the col we want to use as final output (micro and macro have different ones, e.g. facings for micro)


    Returns
    -------
    x_prep_df: pd.DataFrame
        this dataframe has only the exact facings that are chosen (e.g. if the opt. didn't pick
        3 facings for one item that row is excluded)
    """
    # only consider assigned facings, e.g. when the variable was selected from all facing options (e.g. it's a flag)
    x_prep_df = x_df.loc[x_df["solution_value"] >= 1]
    x_prep_df = x_prep_df.drop("solution_value", axis=1)
    x_prep_df[col] = x_prep_df[col].astype(int)

    return x_prep_df


def process_legal_section_break_output(
    opt_q: pd.DataFrame, store_category_dims: pd.DataFrame
) -> pd.DataFrame:
    """This merges current and optimized number of legal breaks and also provides additional info on linear
    space and POG deviaiton from current

    Parameters
    ----------
    opt_q: pd.DataFrame
        df with optimized legal breaks
    store_category_dims: pd.DataFrame
        df with current legal breaks

    Returns
    ----------
    legal_breaks_df: pd.DataFrame
        df with current and optimized legal breaks
    """

    n = get_col_names()

    # TODO doesn't include any constant sections
    opt_q = opt_q.rename(
        {"solution_value": "Opt_Legal_Breaks", "index": n.F_SECTION_MASTER}, axis=1
    )
    store_category_dims = store_category_dims.rename(
        {
            n.F_LEGAL_BREAKS_IN_SECTION: "Cur_Legal_Breaks",
            n.F_THEORETIC_LIN_SPACE: "Cur_Theoretic_Lin_Space",
        },
        axis=1,
    )

    legal_breaks_df = opt_q.merge(
        store_category_dims[
            [
                n.F_STORE_PHYS_NO,
                n.F_SECTION_MASTER,
                "Cur_Legal_Breaks",
                n.F_SECTION_NAME,
                n.F_NO_OF_SHELVES,
                n.F_LINEAR_SPACE_PER_LEGAL_SECTION_BREAK,
                n.F_LEGAL_INCR_WIDTH,
                n.F_CUR_SECTION_WIDTH_DEV,
                n.F_CUR_WIDTH_X_FAC,
                "Cur_Theoretic_Lin_Space",
                n.F_LOCAL_ITEM_WIDTH,
                n.F_MAX_FACINGS_IN_OPT,
            ]
        ],
        on=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER],
        how="inner",
    )

    return legal_breaks_df


def add_in_constant_item_exclusion_facings(
    x_prep_df: pd.DataFrame, exclusions: pd.DataFrame
) -> pd.DataFrame:
    """This function adds the items to the optimized output that were treated constant so that we can treat
    them as fixes output (use exaclty the constant facings numbers)

    Parameters
    ----------
    x_prep_df: pd.DataFrame
        this is the dataframe that has the optmization result per item (how many facings)
    exclusions: pd.DataFrame
        the dataframe that has the constantly treated items and their facings in them


    Returns
    -------
    final_df: pd.DataFrame
        this dataframe with optimized and constant facings results
    """
    # TODO needs to expand
    # Does currenlty NOT add handhelds back into the final output - didn't
    # work anyways with below lines because excl doesn't ahve those in it anymore
    # exclusions are across all categories - so for POC let's just consider PIZZA
    # section_list = cfg["parameters"]["handheld_section_exclusions"]
    # exclusions_new = exclusions.loc[exclusions[n.F_SECTION_NAME].isin(section_list)]

    # get column names
    n = get_col_names()

    keys = [n.F_ITEM_NO, n.F_STORE_PHYS_NO, n.F_FACINGS, n.F_CONST_TREAT]
    exclusions_new = exclusions[keys]

    x_prep_df[n.F_CONST_TREAT] = False

    x_prep_df = x_prep_df[keys]

    if len(exclusions_new) > 0:
        final_df = pd.concat([x_prep_df, exclusions_new], axis=0)
    else:
        final_df = x_prep_df

    return final_df


def _prep_cur_or_opt_facings_output_with_sales_and_margin(
    n: box,
    df_with_facings: pd.DataFrame,
    elasticity_df: pd.DataFrame,
    cluster_dict: dict,
    cur_or_opt: str,
    dep_var: str,
) -> pd.DataFrame:
    """This function prepares the current facings dataframe or the optimized one by
    1) changing the column names to be the same across dataframes so that they can be handled by same subsequent functions
    2) Adds cluster information based on the store info
    3) merges the elasticity (sales or margin) based on which specific facings are used so we can later calculate total sales or margin


    Parameters
    ----------
    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    df_with_facings: pd.DataFrame
        this is the dataframe with either the current facings per item and store or optimized ones
    elasticity_df: pd.DataFrame
        the dataframe that has all sales or margin costs for all possible item facings per cluster
    cluster_dict: dict
        lookup of store physical number to find which cluster it belongs to
    cur_or_opt: str
        string flag whether we deal with cur or opt DF
    dep_var: str
        dependent variable (sales or margin)

    Returns
    -------
    df_with_facings_sales: pd.DataFrame
        this dataframe shows per store in each cluster the facings and their responding sales or margin
    """

    indep_var = n.F_FACINGS
    cur_indep_var = n.F_CUR_FACINGS
    opt_indep_var = n.F_OPT_FACINGS
    facings_merg_cols = [n.F_M_CLUSTER, n.F_ITEM_NO, n.F_FACINGS]
    duplicate_drop = [n.F_STORE_PHYS_NO, n.F_M_CLUSTER, n.F_SECTION_MASTER, n.F_ITEM_NO]

    # since elasticity data is on cluster level, we map store to cluster
    df_with_facings[n.F_M_CLUSTER] = df_with_facings[n.F_STORE_PHYS_NO].map(
        cluster_dict
    )
    # we merge sales (margin data) and cluster info, and need state info and IDX, and section info
    df_with_facings_sales = df_with_facings.merge(
        elasticity_df, on=facings_merg_cols, how="left"
    )
    # TODO here we have nans in section master and sales from constant treated sections above
    # remove duplicates from the POG for category lengths
    df_with_facings_sales = df_with_facings_sales.drop_duplicates(subset=duplicate_drop)
    if cur_or_opt == "CUR":
        df_with_facings_sales = df_with_facings_sales.rename(
            {indep_var: cur_indep_var, dep_var: n.F_CUR + dep_var}, axis=1
        )
    else:
        df_with_facings_sales = df_with_facings_sales.rename(
            {indep_var: opt_indep_var, dep_var: n.F_OPT + dep_var}, axis=1
        )

    return df_with_facings_sales


def process_item_level_output(
    x_prep_df: pd.DataFrame,
    shelve_df: pd.DataFrame,
    elasticity_df: pd.DataFrame,
    cluster_dict: dict,
    product_info_org: pd.DataFrame,
    cluster_item_product_info: pd.DataFrame,
    shelve_master: pd.DataFrame,
    dep_var: str,
) -> pd.DataFrame:
    """In this function we treat current and optimized dfs separately to: 1) calculate total length sales
    and prep output to compare current with optimized item space

    This function is for micro only

    Parameters
    ----------
    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    x_prep_df: pd.DataFrame
        this is the dataframe that has the optimization results with constant items added back in
    shelve_df: pd.DataFrame
        the dataframe with current shelve facings allocations per item
    elasticity_df: pd.DataFrame
        the dataframe that has all sales or margin costs for all possible item facings per cluster
    cluster_dict: dict
        lookup of store physical number to find which cluster it belongs to
    product_info_org: pd.DataFrame
        product info whih has name and category in it with all items (not just ones we optimized for)
    shelve_master: pd.DataFrame
        original shelve info that has all items from plannogram (not just ones we optimized for)
    dep_var: str
        dependent variable (sales or margin)

    Returns
    -------
    final_output: pd.DataFrame
        this dataframe shows per store in each cluster how many current and optimized facings each item has
    """

    # get column names
    n = get_col_names()

    current_shelve_df = shelve_df[n.F_SHELVE_NEW_COLS]
    current_shelve_df[n.F_FACINGS] = current_shelve_df[n.F_FACINGS].astype(int)

    # cluster info is the data frame of product info that has generic item sets that match store cluster section
    # e.g. even if not in current store POG if item was in store cluster it's in here for the store as newly assorted item
    # cluster_item_product_info = cluster_item_product_info.loc[cluster_item_product_info[n.F_CONST_TREAT]==False]

    x_prep_df = x_prep_df[n.F_X_PREP_DF_NEW_COLS]
    if dep_var == n.F_SALES:
        elasticity_df_short = elasticity_df[n.F_ELASTICITY_SALES_NEW_COLS]
    else:
        elasticity_df_short = elasticity_df[n.F_ELASTICITY_MARGIN_NEW_COLS]

    product_info_short = product_info_org[[n.F_ITEM_NO, n.F_ITEM_NAME, n.F_LVL4_NAME]]
    # if we want currently not allocated items but now optimized ones, below needs to be outer join ???
    current_shelve_df = current_shelve_df.merge(
        product_info_short, on=n.F_ITEM_NO, how="left"
    )
    x_prep_df = x_prep_df.merge(product_info_short, on=n.F_ITEM_NO, how="left")
    # we don't want to use current POG here because we may have assorted new items not in store POG but from store cluster
    x_prep_df = x_prep_df.merge(
        cluster_item_product_info[
            [n.F_STORE_PHYS_NO, n.F_SECTION_MASTER, n.F_ITEM_NO]
        ].drop_duplicates(),
        on=[n.F_STORE_PHYS_NO, n.F_ITEM_NO],
        how="inner",
    )

    ###generalize function calculate sales for the current / opt. facings
    shelve_with_sales = _prep_cur_or_opt_facings_output_with_sales_and_margin(
        n=n,
        df_with_facings=current_shelve_df,
        elasticity_df=elasticity_df_short,
        cluster_dict=cluster_dict,
        cur_or_opt="CUR",
        dep_var=dep_var,
    )

    ###same for optimized dataframe
    opt_with_sales = _prep_cur_or_opt_facings_output_with_sales_and_margin(
        n=n,
        df_with_facings=x_prep_df,
        elasticity_df=elasticity_df_short,
        cluster_dict=cluster_dict,
        cur_or_opt="OPT",
        dep_var=dep_var,
    )

    keys = [
        n.F_ITEM_NO,
        n.F_STORE_PHYS_NO,
        n.F_SECTION_MASTER,
        n.F_M_CLUSTER,
        n.F_NEED_STATE,
        n.F_NEED_STATE_IDX,
        n.F_ITEM_NAME,
        n.F_LVL4_NAME,
    ]
    # opt sales here has all items including exclusions but we don't have sales for them
    # so we filter out nans - don't want to do that because we want to show all actually current allocated items
    # opt_with_sales = opt_with_sales.loc[~(opt_with_sales[n.F_OPT + dep_var].isna())]
    final_output = shelve_with_sales.merge(opt_with_sales, on=keys, how="outer")
    # remove items that didn't actually have any sales in POG that are not part of specific store
    final_output = final_output.loc[~(final_output[n.F_CONST_TREAT].isna())]

    # sort columns
    final_output = final_output[
        [
            n.F_M_CLUSTER,
            n.F_STORE_PHYS_NO,
            n.F_SECTION_MASTER,
            n.F_LVL4_NAME,
            n.F_NEED_STATE,
            n.F_NEED_STATE_IDX,
            n.F_ITEM_NO,
            n.F_ITEM_NAME,
            n.F_WIDTH_IN,
            n.F_CONST_TREAT,
            n.F_CUR_FACINGS,
            n.F_OPT_FACINGS,
            n.F_CUR + dep_var,
            n.F_OPT + dep_var,
        ]
    ]

    # clean up data - if opt is assorting items that didn't exist in current version we need to fill it.
    # fill na with 0 if the section has not been optimized for and also couldn't be found in POG
    final_output[n.F_CUR_FACINGS] = final_output[n.F_CUR_FACINGS].fillna(0)
    final_output[n.F_OPT_FACINGS] = final_output[n.F_OPT_FACINGS].fillna(0)
    final_output[n.F_CUR + dep_var] = final_output[n.F_CUR + dep_var].fillna(0)
    final_output[n.F_OPT + dep_var] = final_output[n.F_OPT + dep_var].fillna(0)

    final_output = add_missing_width_for_newly_assorted_items(
        n, final_output, shelve_master
    )

    # filter out items held constant that weren't in original POG to begin with but added as constant items across every store in reg ban
    dont_include = final_output.loc[
        (final_output[n.F_CONST_TREAT] == True)
        & (final_output[n.F_CUR_FACINGS] == 0)
        & (final_output[n.F_OPT_FACINGS] == 0)
        & (final_output[n.F_NEED_STATE].isnull())
    ]
    filtered_output = final_output.merge(
        dont_include, on=final_output.columns.tolist(), indicator=True, how="outer"
    )
    filtered_output = filtered_output.loc[filtered_output["_merge"] == "left_only"]
    filtered_output = filtered_output.drop("_merge", axis=1)

    return filtered_output


def add_missing_width_for_newly_assorted_items(
    n: ColumnNames, final_output: pd.DataFrame, shelve_master: pd.DataFrame
) -> pd.DataFrame:
    """In this function we fill in width of items again for current not assorted but now in opt assorted items
        These items are items that are present (assorted) across stores within a cluster but not necessarily a given
        store. If the store that now receives facings receives facings, the with lookup per that store wouldn't
        work so we fix it here.

    Parameters
    ----------
    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    final_output: pd.DataFrame
        this dataframe shows per store in each cluster how many current and optimized facings each item has
    shelve_master: pd.DataFrame
        the dataframe with current shelve facings allocations per item

    Returns
    -------
    final_output: pd.DataFrame
        this dataframe shows per store in each cluster how many current and optimized facings each item has
        It has now widths corrected that were missing
    """

    shelve_master_fix = shelve_master[[n.F_ITEM_NO, n.F_WIDTH_IN]].drop_duplicates()
    shelve_master_fix = shelve_master_fix.rename({n.F_WIDTH_IN: "Width_fix"}, axis=1)
    final_output = final_output.merge(shelve_master_fix, on=[n.F_ITEM_NO], how="left")
    final_output[n.F_WIDTH_IN] = np.where(
        final_output[n.F_WIDTH_IN].isna(),
        final_output["Width_fix"],
        final_output[n.F_WIDTH_IN],
    )

    final_output = final_output.drop(["Width_fix"], axis=1)

    # now remove duplicates based on width because some of the POG data has
    # two different widths for the same item
    final_output = final_output.drop_duplicates(
        subset=[n.F_SECTION_MASTER, n.F_M_CLUSTER, n.F_STORE_PHYS_NO, n.F_ITEM_NO]
    )

    return final_output


def create_kpis_for_space_productivity(
    log, item_output: pd.DataFrame, dep_var: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """In this function we create the summary KPIs per store. We aggregate facing changes and calculate overall
    sales or margin uplift per store and category

    Parameters
    ----------
    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    log: logger
    item_output: pd.DataFrame
        item level output of current and optimized facings and sales/margin comparison
    elasticity_df: pd.DataFrame
        the dataframe that has all sales or margin costs for all possible section lengths per cluster
    dep_var: str
        dependent variable (sales or margin)

    Returns
    -------
    summary_per_store_and_cat: pd.DataFrame
        this dataframe aggregates current and optimized facingsand sales/margin uplift
    facing_changes: pd.DataFrame
        summary of facing changes
    """

    # get names dict and pass into functions for unified column names
    n = get_col_names()

    cs = round(item_output[n.F_CUR + dep_var].sum(), 2)
    co = round(item_output[n.F_OPT + dep_var].sum(), 2)

    log.info(f"Current {dep_var} are {cs} across all categories and stores")

    log.info(f"Optimized {dep_var} are {co} across all categories and stores")

    # sales per inch
    cs_per_in = round(
        cs / np.multiply(item_output[n.F_WIDTH_IN], item_output[n.F_CUR_FACINGS]).sum(),
        2,
    )
    co_per_in = round(
        co / np.multiply(item_output[n.F_WIDTH_IN], item_output[n.F_OPT_FACINGS]).sum(),
        2,
    )

    log.info(
        f"Current {dep_var} per inch are ${cs_per_in} across all categories and stores"
    )
    log.info(
        f"Optimized {dep_var} per inch are ${co_per_in} across all categories and stores"
    )

    # TODO refactor below two tables into one - they have same multi-key
    # calculate_sales_per_store_and_cat(n, item_output)
    item_output[n.F_UNIQUE_CUR_ASSORT] = np.where(
        item_output[n.F_CUR_FACINGS] > 0, 1, 0
    )
    item_output[n.F_UNIQUE_OPT_ASSORT] = np.where(
        item_output[n.F_OPT_FACINGS] > 0, 1, 0
    )

    # calculate summary of sales per space
    item_output[n.F_CUR_WIDTH_X_FAC] = (
        item_output[n.F_WIDTH_IN] * item_output[n.F_CUR_FACINGS]
    )
    item_output[n.F_OPT_WIDTH_X_FAC] = (
        item_output[n.F_WIDTH_IN] * item_output[n.F_OPT_FACINGS]
    )

    agg_dict = {
        n.F_CUR + dep_var: np.sum,
        n.F_CUR_FACINGS: np.sum,
        n.F_OPT + dep_var: np.sum,
        n.F_OPT_FACINGS: np.sum,
        n.F_UNIQUE_CUR_ASSORT: np.sum,
        n.F_UNIQUE_OPT_ASSORT: np.sum,
        n.F_CUR_WIDTH_X_FAC: np.sum,
        n.F_OPT_WIDTH_X_FAC: np.sum,
    }

    summary_per_store_and_cat = item_output.pivot_table(
        index=[n.F_M_CLUSTER, n.F_STORE_PHYS_NO, n.F_SECTION_MASTER],
        values=[
            n.F_CUR + dep_var,
            n.F_CUR_FACINGS,
            n.F_OPT + dep_var,
            n.F_OPT_FACINGS,
            n.F_UNIQUE_CUR_ASSORT,
            n.F_UNIQUE_OPT_ASSORT,
            n.F_CUR_WIDTH_X_FAC,
            n.F_OPT_WIDTH_X_FAC,
        ],
        aggfunc=agg_dict,
    ).reset_index()

    # calculate metrics
    summary_per_store_and_cat = calculate_metrics_in_aggregated_results_df(
        n, dep_var, summary_per_store_and_cat
    )
    # now create a column that is a flag whether the optimal facings given per item are the same as current
    item_output[n.F_CUR_OPT_FACING_SAME] = np.where(
        item_output[n.F_CUR_FACINGS] == item_output[n.F_OPT_FACINGS], 1, 0
    )
    item_output[n.F_OPT_MINUS_CUR_FACINGS] = (
        item_output[n.F_OPT_FACINGS] - item_output[n.F_CUR_FACINGS]
    )
    agg_dict_fac = {
        n.F_CUR_OPT_FACING_SAME: np.sum,
        n.F_OPT_MINUS_CUR_FACINGS: np.mean,
        n.F_ITEM_NO: pd.Series.nunique,
    }
    facing_changes = item_output.pivot_table(
        index=[n.F_M_CLUSTER, n.F_STORE_PHYS_NO, n.F_SECTION_MASTER],
        values=[n.F_CUR_OPT_FACING_SAME, n.F_OPT_MINUS_CUR_FACINGS, n.F_ITEM_NO],
        aggfunc=agg_dict_fac,
    ).reset_index()
    facing_changes = facing_changes.rename(
        {
            n.F_ITEM_NO: n.F_UNIQUE_ITEMS,
            n.F_OPT_MINUS_CUR_FACINGS: n.F_AVG_OPT_MINUS_CUR_FACINGS,
        },
        axis=1,
    )
    facing_changes[n.F_PERC_ITEMS_W_FACING_DELTA] = (
        facing_changes[n.F_UNIQUE_ITEMS] - facing_changes[n.F_CUR_OPT_FACING_SAME]
    ) / facing_changes[n.F_UNIQUE_ITEMS]
    facing_changes = facing_changes.sort_values(n.F_PERC_ITEMS_W_FACING_DELTA)

    return summary_per_store_and_cat, facing_changes


def calculate_metrics_in_aggregated_results_df(
    n: dict, dep_var: str, df: pd.DataFrame
) -> pd.DataFrame:
    """In this function, after the aggregation to a certain level, we calculate the KPIs like cur to opt sales delta
    The function is reused in the results concatenation for different aggregation levels (e.g. region banner level)

    Parameters
    ----------
    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    dep_var: str
        dependent variable (sales or margin)
    df: pd.DataFrame
        item level output of current and optimized facings and sales/margin comparison


    Returns
    -------
    df: pd.DataFrame
        this dataframe aggregates current and optimized facingsand sales/margin uplift
    """

    # 12 is for inch to foot conversion only
    df[n.F_CUR + dep_var + "_per_feet"] = df[n.F_CUR + dep_var] / (
        df[n.F_CUR_WIDTH_X_FAC] / 12
    )
    df[n.F_OPT + dep_var + "_per_feet"] = df[n.F_OPT + dep_var] / (
        df[n.F_OPT_WIDTH_X_FAC] / 12
    )
    df[n.F_CUR + "avg_" + dep_var + "_per_facing"] = (
        df[n.F_CUR + dep_var] / df[n.F_CUR_FACINGS]
    )
    df[n.F_OPT + "avg_" + dep_var + "_per_facing"] = (
        df[n.F_OPT + dep_var] / df[n.F_OPT_FACINGS]
    )

    df[dep_var + "_cur_opt_delta"] = (
        df[n.F_OPT + dep_var] - df[n.F_CUR + dep_var]
    ) / df[n.F_CUR + dep_var]

    df["Space_cur_opt_delta"] = (
        df[n.F_OPT_WIDTH_X_FAC] - df[n.F_CUR_WIDTH_X_FAC]
    ) / df[n.F_CUR_WIDTH_X_FAC]

    return df


def determine_store_and_non_constant_section_tuple(n: dict, item_output) -> tuple:
    """We want to do the unit proportions adjustment only on items that are not treatet constant
    so we need to find the store and section tuple

    Parameters
    ----------

    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    item_output: pd.DataFrame
        item output with current and optimized facings
    Returns
    -------
    store_list_section_list: tuple
        This tuple is store and section tuple of all non-constantly treated sections
    """

    # TODO: assess if this is normal: sometimes n.F_CONST_TREAT is NaN, replacing with False
    # item_output[n.F_CONST_TREAT] = item_output[n.F_CONST_TREAT].fillna(False)

    section_store_unique = item_output.loc[item_output[n.F_CONST_TREAT] == False]

    store_list_section_list = list(
        tuple(x)
        for x in section_store_unique[[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER]]
        .drop_duplicates()
        .values
    )

    return store_list_section_list


def add_dependent_var_back_in_and_calculate_new_sales_or_margin(
    n: dict, dep_var: str, df_with_ns_totals: pd.DataFrame, elasticity_df: pd.DataFrame
) -> pd.DataFrame:
    """This function finds the width of need states and ignores constant treated items

    This is for micro only

    Parameters
    ----------
    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    df_with_ns_totals: pd.DataFrame
        df with facing allocations
    dep_var: str
        dependent variable, either sales or margin
    elasticity_df: pd.DataFrame
        elast sales or margin by number of facings from curves

    Returns
    -------
    item_output_proportions: pd.DataFrame
        This df is the final output of unit proportions allocated facings with dependent var amounts in df
    """

    # now merge the optimal sales from elasticity file in so we have the same output format and can calculate final metrics
    keys = [n.F_SECTION_MASTER, n.F_M_CLUSTER, n.F_ITEM_NO, n.F_FACINGS]
    df_with_ns_totals = df_with_ns_totals.merge(
        elasticity_df[
            [
                n.F_SECTION_MASTER,
                n.F_M_CLUSTER,
                n.F_ITEM_NO,
                n.F_FACINGS,
                dep_var,
            ]
        ],
        on=keys,
        how="left",
    )
    df_with_ns_totals[n.F_OPT + dep_var] = df_with_ns_totals[dep_var]
    df_with_ns_totals[n.F_OPT_FACINGS] = df_with_ns_totals[n.F_FACINGS]

    if dep_var == n.F_SALES:
        item_output_proportions = df_with_ns_totals[n.F_SALES_PROPORTION_FINAL_COLS]
    else:
        item_output_proportions = df_with_ns_totals[n.F_MARGIN_PROPORTION_FINAL_COLS]

    return item_output_proportions


def add_historic_sales_or_margin_and_units(
    bay_data: pd.DataFrame,
    item_output: pd.DataFrame,
    cluster_assignment: pd.DataFrame,
) -> pd.DataFrame:
    """This function reads the historic sales or margin to model and adds it to item_output
    to spread we use the item count instead of sales which is more robust

    This is for micro only

    Parameters
    ----------
    cfg: optimization config
    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    cluster_assignment: pd.DataFrame
        this dataframe has store phys to cluster (merged) mapping in it
    item_output: pd.DataFrame
        original current and optimized solution dataframe
    spark: sparkSession

    Returns
    -------
    item_output_new: pd.DataFrame
        This df has the historic sales and margin per facing as well as units sold
    """
    # get names dict and pass into functions for unified column names
    n = get_col_names()
    # pdf_bd = pdf_bay_data
    #
    # cols = [
    #     "NATIONAL_BANNER_DESC",
    #     "REGION",
    #     "SECTION_MASTER",
    #     "STORE_PHYSICAL_LOCATION_NO",
    #     "ITEM_NO",
    #     "Sales",
    #     "Facings",
    #     "E2E_MARGIN_TOTAL",
    #     "need_state",
    #     "Item_Count",
    # ]
    #
    # pdf_bd = pdf_bd[cols]
    #
    # dims = ["need_state", "STORE_PHYSICAL_LOCATION_NO", "ITEM_NO"]
    # pdf_bd = pdf_bd.drop_duplicates(subset=dims)
    #
    # rename_conf = {
    #     "NATIONAL_BANNER_DESC": n.F_BANNER,
    #     "REGION": n.F_REGION_DESC,
    # }
    #
    # pdf_bd = pdf_bd.rename(columns=rename_conf, errors="raise")
    #
    # mask = f"{n.F_BANNER}=='{banner}' & {n.F_REGION_DESC}=='{region}'"
    # pdf_bd = pdf_bd.query(mask)
    #
    # bay_data = pdf_bd
    # if "E2E_MARGIN_TOTAL" in bay_data.columns:
    #     bay_data.rename(
    #         columns={"E2E_MARGIN_TOTAL": n.F_MARGIN}, inplace=True, errors="raise"
    #     )
    #
    # # make column name in snake case
    # bay_data.rename(
    #     columns=dict(
    #         zip(bay_data.columns, [strip_except_alpha_num(x) for x in bay_data.columns])
    #     ),
    #     inplace=True,
    # )

    # drop any duplicates
    bay_data = bay_data.drop_duplicates(
        subset=[
            n.F_NEED_STATE,
            n.F_SECTION_MASTER,
            n.F_STORE_PHYS_NO,
            n.F_ITEM_NO,
            n.F_FACINGS,
        ]
    )

    # aggregate sales or margin across all facings now per store
    keys = [
        n.F_REGION_DESC,
        n.F_SECTION_MASTER,
        n.F_NEED_STATE,
        n.F_STORE_PHYS_NO,
        n.F_ITEM_NO,
    ]
    summary = bay_data.pivot_table(
        index=keys, values=n.F_ITEM_COUNT, aggfunc=np.sum
    ).reset_index()

    # merge store cluster in so we can aggregate on it
    summary = summary.merge(cluster_assignment, on=[n.F_STORE_PHYS_NO], how="left")

    # aggregate avg. sales across all stores now
    keys_avg = [n.F_M_CLUSTER, n.F_NEED_STATE, n.F_ITEM_NO]
    summary_avg = summary.pivot_table(
        index=keys_avg, values=n.F_ITEM_COUNT, aggfunc=np.mean
    ).reset_index()

    # now we merge historic sales or margin into the item output
    item_output_new = item_output.merge(summary_avg, on=keys_avg, how="left")
    # If there weren't any historic sales we fill the nas with 0
    if n.F_ITEM_COUNT not in item_output_new.columns:
        item_output_new[n.F_ITEM_COUNT] = 0
    item_output_new[n.F_ITEM_COUNT] = item_output_new[n.F_ITEM_COUNT].fillna(0)

    return item_output_new


def add_region_banner_dept_cols_to_df(
    region: str,
    banner: str,
    dept: str,
    df: pd.DataFrame,
) -> pd.DataFrame:
    """This function adds region banner dept to dataframe

    Parameters
    ----------
    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    region: str
        the region over which we create sets
    banner: str
        the banner over which we create sets
    dept: str
        the department over which we create the sets (group of sections)

    Returns
    ----------
    df: pd.DataFrame
        df with region banner dept columns
    """

    # get names dict and pass into functions for unified column names
    n = get_col_names()

    lookup_dict = {n.F_REGION_DESC: region, n.F_BANNER: banner, n.F_DEPARTMENT: dept}

    for col in [n.F_REGION_DESC, n.F_BANNER, n.F_DEPARTMENT]:
        if col not in df.columns:
            df[col] = lookup_dict[col]

    return df


def calculate_region_banner_dept_store_summary(n, dep_var, item_output) -> pd.DataFrame:
    """In this function we create the summary KPIs on different levels of aggregation, e.g. on region banner dept level, or region banner dept store etc.

    Parameters
    ----------
    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    dep_var: str
        dependent variable (sales or margin)
    item_output: pd.DataFrame
        item level output of current and optimized facings and sales/margin comparison

    Returns
    -------
    region_banner_dept_store_summary: pd.DataFrame
        this dataframe aggregates current and optimized facings and sales/margin uplift
    region_banner_dept_summary: pd.DataFrame
        this dataframe aggregates current and optimized facings and sales/margin uplift
    region_banner_summary: pd.DataFrame
        this dataframe aggregates current and optimized facings and sales/margin uplift
    """

    # calculate_sales_per_store_and_cat(n, item_output)
    item_output[n.F_UNIQUE_CUR_ASSORT] = np.where(
        item_output[n.F_CUR_FACINGS] > 0, 1, 0
    )
    item_output[n.F_UNIQUE_OPT_ASSORT] = np.where(
        item_output[n.F_OPT_FACINGS] > 0, 1, 0
    )

    # calculate summary of sales per space
    item_output[n.F_CUR_WIDTH_X_FAC] = (
        item_output[n.F_WIDTH_IN] * item_output[n.F_CUR_FACINGS]
    )
    item_output[n.F_OPT_WIDTH_X_FAC] = (
        item_output[n.F_WIDTH_IN] * item_output[n.F_OPT_FACINGS]
    )

    agg_dict = {
        n.F_CUR + dep_var: np.sum,
        n.F_CUR_FACINGS: np.sum,
        n.F_OPT + dep_var: np.sum,
        n.F_OPT_FACINGS: np.sum,
        n.F_UNIQUE_CUR_ASSORT: np.sum,
        n.F_UNIQUE_OPT_ASSORT: np.sum,
        n.F_CUR_WIDTH_X_FAC: np.sum,
        n.F_OPT_WIDTH_X_FAC: np.sum,
    }

    agg_value_list = [
        n.F_CUR + dep_var,
        n.F_CUR_FACINGS,
        n.F_OPT + dep_var,
        n.F_OPT_FACINGS,
        n.F_UNIQUE_CUR_ASSORT,
        n.F_UNIQUE_OPT_ASSORT,
        n.F_CUR_WIDTH_X_FAC,
        n.F_OPT_WIDTH_X_FAC,
    ]

    # region_banner_dept_store_summary
    region_banner_dept_store_summary = item_output.pivot_table(
        index=[
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_DEPARTMENT,
            n.F_M_CLUSTER,
            n.F_STORE_PHYS_NO,
        ],
        values=agg_value_list,
        aggfunc=agg_dict,
    ).reset_index()

    region_banner_dept_store_summary = calculate_metrics_in_aggregated_results_df(
        n, dep_var, region_banner_dept_store_summary
    )

    # region_banner_dept_summary
    region_banner_dept_summary = item_output.pivot_table(
        index=[n.F_REGION_DESC, n.F_BANNER, n.F_DEPARTMENT, n.F_M_CLUSTER],
        values=agg_value_list,
        aggfunc=agg_dict,
    ).reset_index()

    region_banner_dept_summary = calculate_metrics_in_aggregated_results_df(
        n, dep_var, region_banner_dept_summary
    )

    # region_banner_dept_summary
    region_banner_summary = item_output.pivot_table(
        index=[n.F_REGION_DESC, n.F_BANNER, n.F_M_CLUSTER],
        values=agg_value_list,
        aggfunc=agg_dict,
    ).reset_index()

    region_banner_summary = calculate_metrics_in_aggregated_results_df(
        n, dep_var, region_banner_summary
    )

    return (
        region_banner_dept_store_summary,
        region_banner_dept_summary,
        region_banner_summary,
    )


def determine_supplier_own_brands_space(
    item_output: pd.DataFrame,
    request: pd.DataFrame,
    mapping: pd.DataFrame,
) -> pd.DataFrame:
    """This calculates the space by supplier and total space per store section to calculate the percentage of space the supplier received.
    We then compare this with the requested space to see which was over and under

    Parameters
    ----------
    item_output: pd.DataFrame
        df of item output coming out of the item_output level function, unit proportions, or
        supplier adjustment with current and optimal facings and sales/margin
    request: pd.DataFrame
        df with merchant requests for supplier own brands combinations
    mapping: pd.DataFrame
        df with mapping of items for a particular supplier own brands ID

    Returns
    ----------
    supplier_own_brands_analysis: pd.DataFrame
        df with space analysis of total space, percentage requested of space and actual percentage
    """
    n = get_col_names()

    # calculate summary of sales per space
    item_output[n.F_CUR_WIDTH_X_FAC] = (
        item_output[n.F_WIDTH_IN] * item_output[n.F_CUR_FACINGS]
    )
    item_output[n.F_OPT_WIDTH_X_FAC] = (
        item_output[n.F_WIDTH_IN] * item_output[n.F_OPT_FACINGS]
    )

    # merge in supplier mapping onto item output on item level df
    item_output = item_output.merge(
        mapping, on=[n.F_SECTION_MASTER, n.F_ITEM_NO], how="outer"
    )

    # exclude any items that are in mapping that are not in current or opt at all
    item_output = item_output.loc[~(item_output[n.F_ITEM_NAME].isna())]

    # aggregate totals to get withs
    agg_dict = {
        n.F_CUR_WIDTH_X_FAC: np.sum,
        n.F_OPT_WIDTH_X_FAC: np.sum,
    }

    aggregate_widths_all = item_output.pivot_table(
        index=[n.F_M_CLUSTER, n.F_STORE_PHYS_NO, n.F_SECTION_MASTER], aggfunc=agg_dict
    ).reset_index()

    aggregate_widths_all = aggregate_widths_all.rename(
        {
            n.F_CUR_WIDTH_X_FAC: n.F_CUR_WIDTH_X_FAC + "_Total",
            n.F_OPT_WIDTH_X_FAC: n.F_OPT_WIDTH_X_FAC + "_Total",
        },
        axis=1,
    )

    totals_by_supplier_ob_id = item_output.pivot_table(
        index=[
            n.F_M_CLUSTER,
            n.F_STORE_PHYS_NO,
            n.F_SECTION_MASTER,
            n.F_SUPPLIER_OB_ID,
        ],
        aggfunc=agg_dict,
    ).reset_index()

    totals_by_supplier_ob_id = totals_by_supplier_ob_id.merge(
        aggregate_widths_all,
        on=[n.F_M_CLUSTER, n.F_STORE_PHYS_NO, n.F_SECTION_MASTER],
        how="left",
    )
    # calculate percentage space met
    try:
        # we try if a specific region banner dept doesn't have these constraints the col doesn't exist
        totals_by_supplier_ob_id["Opt_Percent_Space"] = (
            totals_by_supplier_ob_id[n.F_OPT_WIDTH_X_FAC]
            / totals_by_supplier_ob_id[n.F_OPT_WIDTH_X_FAC + "_Total"]
        )
    except KeyError:
        totals_by_supplier_ob_id[n.F_OPT_WIDTH_X_FAC] = 0
        totals_by_supplier_ob_id[n.F_CUR_WIDTH_X_FAC] = 0
        totals_by_supplier_ob_id["Opt_Percent_Space"] = (
            totals_by_supplier_ob_id[n.F_OPT_WIDTH_X_FAC]
            / totals_by_supplier_ob_id[n.F_OPT_WIDTH_X_FAC + "_Total"]
        )

        # rename requst column for clarity before merge
    request = request.rename({n.F_PERCENTAGE_SPACE: "Requested_Percent_Space"}, axis=1)

    # merge in space request to compare
    totals_by_supplier_ob_id = totals_by_supplier_ob_id.merge(
        request, on=[n.F_SECTION_MASTER, n.F_SUPPLIER_OB_ID], how="outer"
    )

    # order columns
    supplier_own_brands_analysis = totals_by_supplier_ob_id[
        n.F_SUPPLIER_OWN_BRANDS_ANALYSIS_COLS
    ]

    return supplier_own_brands_analysis


def determine_supplier_own_brands_space_post_step(
    item_output: pd.DataFrame,
    request: pd.DataFrame,
    mapping: pd.DataFrame,
    original_analysis: pd.DataFrame,
    step: str,
) -> pd.DataFrame:
    """This function calculates the percentage met for the item level output post unit proportions and
    merges those results with the original anlaysis before unit proportions. It is a wrapper function for
    the original space analysis function determine_supplier_own_brands_space and tehn renames columns

    Parameters
    ----------
    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    item_output: pd.DataFrame
        df of item output coming out of the item_output level function, unit proportions, or
        supplier adjustment with current and optimal facings and sales/margin
    request: pd.DataFrame
        df with merchant requests for supplier own brands combinations
    mapping: pd.DataFrame
        df with mapping of items for a particular supplier own brands ID
    original_analysis: pd.DataFrame
        df with original analysis coming out of determine_supplier_own_brands_space
    step: str
        whether the column names should be renamed to be after unit proportions anlaysis or after supplier adjustement

    Returns
    ----------
    supplier_own_brands_analysis: pd.DataFrame
        df with space analysis of total space, percentage requested of space and actual percentage post unit props
    """
    n = get_col_names()

    unit_proportions_analysis = determine_supplier_own_brands_space(
        item_output=item_output,
        request=request,
        mapping=mapping,
    )
    if step == "post_unit_prop":
        unit_proportions_analysis = unit_proportions_analysis.rename(
            {
                "Opt_Percent_Space": "Post_Unit_Prop_Opt_Percent_Space",
                n.F_OPT_WIDTH_X_FAC: "Post_Unit_" + n.F_OPT_WIDTH_X_FAC,
                n.F_OPT_WIDTH_X_FAC
                + "_Total": "Post_Unit_"
                + n.F_OPT_WIDTH_X_FAC
                + "_Total",
            },
            axis=1,
        )
        new_col_list_for_unit_props = [
            n.F_STORE_PHYS_NO,
            n.F_SECTION_MASTER,
            n.F_SUPPLIER_OB_ID,
            "Post_Unit_Prop_Opt_Percent_Space",
            "Post_Unit_" + n.F_OPT_WIDTH_X_FAC,
            "Post_Unit_" + n.F_OPT_WIDTH_X_FAC + "_Total",
        ]
    elif step == "post_unit_prop_adj":
        unit_proportions_analysis = unit_proportions_analysis.rename(
            {
                "Opt_Percent_Space": "Final_Opt_Percent_Space",
                n.F_OPT_WIDTH_X_FAC: "Final_" + n.F_OPT_WIDTH_X_FAC,
                n.F_OPT_WIDTH_X_FAC
                + "_Total": "Final_"
                + n.F_OPT_WIDTH_X_FAC
                + "_Total",
            },
            axis=1,
        )
        new_col_list_for_unit_props = [
            n.F_STORE_PHYS_NO,
            n.F_SECTION_MASTER,
            n.F_SUPPLIER_OB_ID,
            "Final_Opt_Percent_Space",
            "Final_" + n.F_OPT_WIDTH_X_FAC,
            "Final_" + n.F_OPT_WIDTH_X_FAC + "_Total",
        ]

    combined_analysis = original_analysis.merge(
        unit_proportions_analysis[new_col_list_for_unit_props],
        on=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER, n.F_SUPPLIER_OB_ID],
        how="left",
    )

    return combined_analysis


def adapt_unit_prop_item_output_to_comply_with_supplier_own_brands_requests(
    max_facings: int,
    diff_facings_in_unit_prop: int,
    item_output: pd.DataFrame,
    space_analysis: pd.DataFrame,
    request: pd.DataFrame,
    mapping: pd.DataFrame,
    elasticity_df: pd.DataFrame,
    facings_per_section_summary: pd.DataFrame,
    dep_var: str,
) -> pd.DataFrame:
    """This function is the wrapper function to adjust the unit proportions again with either removing or adding
    space in case supplier percentages have not been met. The function is called after the unit proportions are run and has the goal of ensuring that the
    supplier own brands request is truly met even if the unit props may have not allocated the space based on unit proportions per need state

    Parameters
    ----------
    max_facings: int
        max facings generated from elasticities
    diff_facings_in_unit_prop: int
        this is added to previously determined max facings which already added the diff above. e.g. should be the same number just inverses
    item_output: pd.DataFrame
        df of item output coming out of the item_output level function, unit proportions, or
        supplier adjustment with current and optimal facings and sales/margin
    space_analysis: pd.DataFrame
            supplier_own_brands_analysis: pd.DataFrame
        df with space analysis of total space, percentage requested of space and actual percentage post unit props
    request: pd.DataFrame
        df with merchant requests for supplier own brands combinations
    mapping: pd.DataFrame
        df with mapping of items for a particular supplier own brands ID
    facings_per_section_summary: pd.DataFrame
        dataframe that has the summary of max facings that should be allocated per section master
    elasticity_df: pd.DataFrame
        df with elasticity curves for the removal ranking of items of supliers that received to much space or items
        where we can take space from when we added space for supplier OBs where space fell short of requested
    dep_var: str
        sales or margin

    Returns
    ----------
    item_output: pd.DataFrame
        df of item output coming out of the item_output level function, unit proportions, or
        supplier adjustment with current and optimal facings and sales/margin
        - adjusted for supplier requests again -
    """
    n = get_col_names()

    elasticity_df = elasticity_df.drop_duplicates(
        subset=[n.F_SECTION_MASTER, n.F_M_CLUSTER, n.F_ITEM_NO, n.F_FACINGS]
    )

    # add in max number of facings so we can limit to this number
    # adjust dynamic max facings by difference addition for unit proportions again
    diff = diff_facings_in_unit_prop  # cfg["parameters"]["difference_of_facings_to_allocate_in_unit_proportions"]
    # TODO do this step in data prep as second column (e.g. max in opt, max in unit props) and ensure that resulting # fac. is <= max facings
    # assert diff > 0, "Diff to add to dynamic max facings has to be positive"
    facings_per_section_summary[n.F_MAX_FACINGS_IN_OPT] = (
        facings_per_section_summary[n.F_MAX_FACINGS_IN_OPT] + diff
    )
    facings_per_section_summary = facings_per_section_summary[
        [n.F_STORE_PHYS_NO, n.F_SECTION_MASTER, n.F_MAX_FACINGS_IN_OPT]
    ]

    # join max facings in opt into df
    item_output = item_output.merge(
        facings_per_section_summary,
        on=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER],
        how="inner",
    )

    # calculate percentage deviation of space given and requested
    space_analysis["space_percent_abs_deviation"] = (
        space_analysis["Post_Unit_Prop_Opt_Percent_Space"]
        - space_analysis["Requested_Percent_Space"]
    )

    # determine additional space to add for supplier OBs where space is lower than requested
    space_analysis[n.F_SPACE_TO_ADD] = (
        -space_analysis["space_percent_abs_deviation"]
        * space_analysis["Post_Unit_Opt_Width_X_Facing_Total"]
    )

    iterate_store_section = space_analysis.loc[
        space_analysis[n.F_STORE_PHYS_NO].notna()
    ]
    iterate_store_section = iterate_store_section.loc[
        iterate_store_section["space_percent_abs_deviation"] < 0
    ]
    store_list_section_list = list(
        tuple(x)
        for x in iterate_store_section[[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER]]
        .drop_duplicates()
        .values
    )

    # retain item output that doesn't have to go through this
    # find supplier OBs where space is lower than requested
    supp_obs_under_request = space_analysis.loc[
        space_analysis["space_percent_abs_deviation"] < 0
    ]
    retainer = supp_obs_under_request[
        [n.F_SECTION_MASTER, n.F_STORE_PHYS_NO]
    ].drop_duplicates()
    item_output_to_retain = item_output.merge(
        retainer,
        on=[n.F_SECTION_MASTER, n.F_STORE_PHYS_NO],
        how="outer",
        indicator=True,
    )
    item_output_to_retain = item_output_to_retain.loc[
        item_output_to_retain["_merge"] == "left_only"
    ]

    # filter down on item output where we add or remove space to then do all adjustments on store section level again
    store_df_list = []
    for store, section in store_list_section_list:
        # only feed in item output for a store section that needs to be altered
        df = item_output.loc[
            (item_output[n.F_STORE_PHYS_NO] == store)
            & (item_output[n.F_SECTION_MASTER] == section)
        ]
        store_cluster = df[n.F_M_CLUSTER].iloc[0]
        # only feed in supplier requests relevant for store section
        space_analysis_store_section = space_analysis.loc[
            (space_analysis[n.F_STORE_PHYS_NO] == store)
            & (space_analysis[n.F_SECTION_MASTER] == section)
        ]
        mapping_filtered = mapping.loc[mapping[n.F_SECTION_MASTER] == section]
        request_filtered = request.loc[request[n.F_SECTION_MASTER] == section]

        elasticity_df_filtered = elasticity_df.loc[
            (elasticity_df[n.F_SECTION_MASTER] == section)
            & ((elasticity_df[n.F_M_CLUSTER] == store_cluster))
        ]

        (
            item_output_added_facings_st_sec,
            space_added,
        ) = store_section_facing_add_space_to_low_supplier_percentages(
            df=df,
            space_analysis=space_analysis_store_section,
            mapping=mapping_filtered,
        )

        # calculate how much space we over allocated compared to original space per section
        space_analysis_added_facings_st_sec = determine_supplier_own_brands_space(
            item_output=item_output_added_facings_st_sec,
            request=request_filtered,
            mapping=mapping_filtered,
        )

        # prep space to add column again
        # determine additional space to add for supplier OBs where space is lower than requested
        space_analysis_added_facings_st_sec[n.F_SPACE_TO_ADD] = (
            -(
                space_analysis_added_facings_st_sec["Opt_Percent_Space"]
                - space_analysis_added_facings_st_sec["Requested_Percent_Space"]
            )
            * space_analysis_added_facings_st_sec[n.F_OPT_WIDTH_X_FAC + "_Total"]
        )

        # add supplier ob id to item output before we add facings in next algo
        # extend supple request + mapping to get to item matching
        request_items = space_analysis_added_facings_st_sec.merge(
            mapping[[n.F_ITEM_NO, n.F_SUPPLIER_OB_ID]],
            on=[n.F_SUPPLIER_OB_ID],
            how="left",
        )
        # merge space to add on item numbers
        item_output_added_facings_st_sec = item_output_added_facings_st_sec.merge(
            request_items[[n.F_ITEM_NO, n.F_SUPPLIER_OB_ID, n.F_SPACE_TO_ADD]],
            on=[n.F_ITEM_NO],
            how="left",
        )
        # TODO ensure that we check how much we removed
        item_output_with_facings_removed = remove_space(
            max_facings=max_facings,
            n=n,
            items_w_suppliers=item_output_added_facings_st_sec,
            elasticity=elasticity_df_filtered,
            space_to_remove=space_added,
            dep_var=dep_var,
        )
        # update sales or margin numbers based on final facings
        # function expects facings column instead of opt facings
        item_output_with_facings_removed[
            n.F_FACINGS
        ] = item_output_with_facings_removed[n.F_OPT_FACINGS]
        item_output_proportions = (
            add_dependent_var_back_in_and_calculate_new_sales_or_margin(
                n, dep_var, item_output_with_facings_removed, elasticity_df_filtered
            )
        )
        store_df_list.append(item_output_proportions)

    item_output_final = pd.concat(store_df_list, ignore_index=True)

    # add in item output retainer
    item_output_to_retain = item_output_to_retain.drop(
        [n.F_ITEM_COUNT, "_merge", n.F_MAX_FACINGS_IN_OPT], axis=1
    )
    item_output_final = pd.concat([item_output_to_retain, item_output_final], axis=0)
    # prep item output final
    item_output_final[n.F_OPT_WIDTH_X_FAC] = (
        item_output_final[n.F_OPT_FACINGS] * item_output_final[n.F_WIDTH_IN]
    )
    item_output_final[n.F_CUR_WIDTH_X_FAC] = (
        item_output_final[n.F_CUR_FACINGS] * item_output_final[n.F_WIDTH_IN]
    )

    return item_output_final


def remove_space(
    max_facings: int,
    n: dict,
    items_w_suppliers: pd.DataFrame,
    elasticity: pd.DataFrame,
    space_to_remove: float,
    dep_var: str,
):
    """
    Removes space from items with suppliers over the supplier constraint and items from suppliers without constraints.

    Parameters
    ----------
    items_w_suppliers: pd.DataFrame
        table at store-section-item level with columns including current optimal facings, supplier ID, and whether they are eligible to be removed
    elasticity: pd.DataFrame
        a long (melted) table of elasticity outputs, with one row being a cluster-item-facing value
    space_to_remove: float
        amount of space in inches to remove
    dep_var: str
        sales or margin

    Returns
    -------
    df_output : pd.DataFrame
      updated version of items_w_suppliers with updated facing counts in `Optim_Facings`
    """

    # First fill NA for items from suppliers without constraints
    items_w_suppliers[n.F_SUPPLIER_OB_ID] = items_w_suppliers[
        n.F_SUPPLIER_OB_ID
    ].fillna(
        "-1"
    )  # all items will be 'grouped under' supplier ID -1
    items_w_suppliers[n.F_SPACE_TO_ADD] = items_w_suppliers[n.F_SPACE_TO_ADD].fillna(
        -9999
    )  # supplier ID -1 has 9999 inches to remove
    assert items_w_suppliers["Supplier_Ob_Id"].dtype == object
    assert items_w_suppliers[n.F_SPACE_TO_ADD].dtype == float
    assert items_w_suppliers["removable"].dtype == int
    assert items_w_suppliers["Width"].dtype == float
    assert elasticity["Need_State"].dtype == object
    assert items_w_suppliers["Need_State"].dtype == object
    assert elasticity["Item_No"].dtype == object
    assert items_w_suppliers["Item_No"].dtype == object
    # assert items_w_suppliers["Optim_Facings"].dtype == float
    assert items_w_suppliers["Item_Count"].dtype == float
    assert items_w_suppliers[f"Opt_{dep_var}"].dtype == float
    assert elasticity["Facings"].dtype == np.int64
    assert elasticity[f"{dep_var}"].dtype == float
    # Reduce our table a bit, looking only at removable items
    items_to_remove = items_w_suppliers[items_w_suppliers["removable"] == 1]
    items_to_remove = items_to_remove[
        [
            n.F_M_CLUSTER,
            n.F_STORE_PHYS_NO,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
            n.F_ITEM_NO,
            n.F_ITEM_COUNT,
            f"Opt_{dep_var}",
            n.F_OPT_FACINGS,
            n.F_SUPPLIER_OB_ID,
            n.F_SPACE_TO_ADD,
        ]
    ]
    items_to_remove[n.F_ITEM_NO] = items_to_remove[n.F_ITEM_NO].astype(str)

    # Join elasticities and calculate sales diff between optimal facing and that of one facing removed
    items_to_remove_elas = items_to_remove.merge(
        elasticity, on=["M_Cluster", "Section_Master", "Need_State", n.F_ITEM_NO]
    )
    items_to_remove_elas = items_to_remove_elas[
        items_to_remove_elas[n.F_OPT_FACINGS] > 0
    ]
    items_to_remove_elas = items_to_remove_elas[
        items_to_remove_elas[n.F_OPT_FACINGS] < max_facings
    ]
    items_to_remove_elas["Optim_Facings_One_Less"] = (
        items_to_remove_elas[n.F_OPT_FACINGS] - 1
    )
    items_to_remove_elas = items_to_remove_elas[
        (items_to_remove_elas[n.F_FACINGS] == items_to_remove_elas[n.F_OPT_FACINGS])
        | (
            items_to_remove_elas[n.F_FACINGS]
            == items_to_remove_elas["Optim_Facings_One_Less"]
        )
    ]
    curr_facings = items_to_remove_elas[
        items_to_remove_elas[n.F_OPT_FACINGS] == items_to_remove_elas[n.F_FACINGS]
    ]
    curr_facings = curr_facings.pivot_table(
        index=[
            n.F_M_CLUSTER,
            n.F_STORE_PHYS_NO,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
            n.F_ITEM_NO,
            n.F_ITEM_COUNT,
            n.F_OPT_FACINGS,
        ],
        values=dep_var,
    ).reset_index()
    curr_facings = curr_facings.rename({dep_var: f"optim_{dep_var}"}, axis=1)
    one_less_facings = items_to_remove_elas[
        items_to_remove_elas["Optim_Facings_One_Less"]
        == items_to_remove_elas[n.F_FACINGS]
    ]
    one_less_facings = one_less_facings.pivot_table(
        index=[
            n.F_M_CLUSTER,
            n.F_STORE_PHYS_NO,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
            n.F_ITEM_NO,
            n.F_ITEM_COUNT,
            "Optim_Facings_One_Less",
        ],
        values=dep_var,
    ).reset_index()
    one_less_facings = one_less_facings.rename(
        {dep_var: f"optim_one_less_{dep_var}"}, axis=1
    )
    pdf_potential_removal = curr_facings.merge(
        one_less_facings,
        on=[
            n.F_M_CLUSTER,
            n.F_STORE_PHYS_NO,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
            n.F_ITEM_NO,
            n.F_ITEM_COUNT,
        ],
    )
    pdf_potential_removal[f"{dep_var}_diff"] = (
        pdf_potential_removal[f"optim_{dep_var}"]
        - pdf_potential_removal[f"optim_one_less_{dep_var}"]
    )
    # always call it sales diff here so function apply works without extra str passed in
    pdf_potential_removal = pdf_potential_removal.rename(
        {f"{dep_var}_diff": "sales_diff"}, axis=1
    )
    # Run the BY ranking algorithm
    rankings = (
        pdf_potential_removal.groupby(
            [n.F_M_CLUSTER, n.F_STORE_PHYS_NO, n.F_SECTION_MASTER]
        )
        .apply(get_ranking)
        .reset_index()
    )
    rankings["ITEM_TO_REMOVE"] = rankings["ITEM_TO_REMOVE"].astype(str)
    # Get suppliers' space if they are above 0
    supplier_spaces = items_to_remove.drop_duplicates(
        [n.F_SUPPLIER_OB_ID, n.F_SPACE_TO_ADD]
    )[[n.F_SUPPLIER_OB_ID, n.F_SPACE_TO_ADD]]
    supplier_references = dict(
        zip(supplier_spaces[n.F_SUPPLIER_OB_ID], supplier_spaces[n.F_SPACE_TO_ADD])
    )

    # Remove items iteratively
    items_removed = set()
    item_removal_list = rankings.copy()  # running list of items still left to remove
    while space_to_remove > 0:
        item_removal_list = item_removal_list.sort_values("ORDER")
        if len(item_removal_list) > 0:
            item_to_remove = item_removal_list.iloc[0]
            item_no_to_remove = item_to_remove["ITEM_TO_REMOVE"]

            item_lookup = items_w_suppliers[
                items_w_suppliers[n.F_ITEM_NO] == item_no_to_remove
            ]
            item_width = item_lookup[n.F_WIDTH_IN].iloc[0]
            supplier = item_lookup[n.F_SUPPLIER_OB_ID].iloc[0]

            if supplier_references[supplier] + item_width < 0:
                space_to_remove -= item_width
                supplier_references[supplier] += item_width
                print(f"removed item {item_no_to_remove}")
                items_removed.add(item_no_to_remove)

            item_removal_list = item_removal_list[
                item_removal_list["ITEM_TO_REMOVE"] != item_no_to_remove
            ]
        else:
            break

    # Update facings of items removed
    items_w_suppliers["remove_facing"] = np.where(
        items_w_suppliers[n.F_ITEM_NO].isin(items_removed),
        1,
        0,
    )

    # remove facings actually
    items_w_suppliers[n.F_OPT_FACINGS] = (
        items_w_suppliers[n.F_OPT_FACINGS] - items_w_suppliers["remove_facing"]
    )
    items_w_suppliers = items_w_suppliers.drop(["remove_facing"], axis=1)
    return items_w_suppliers


def get_min(all_items_ns, applicable_need_states):
    curr_ns_min, curr_item_min, facings_min = 0, 0, 0

    curr_min_diff = 1000000
    for ns, ns_items in all_items_ns.items():
        item_ref = ns_items[0][0]
        diff_ref = ns_items[0][1]
        facings_ref = ns_items[0][2]
        if diff_ref < curr_min_diff and ns in applicable_need_states:
            curr_ns_min = ns
            curr_item_min = item_ref
            facings_min = facings_ref
            curr_min_diff = diff_ref
    return curr_ns_min, curr_item_min, facings_min, curr_min_diff


def get_ranking(df):
    store = df.Store_Physical_Location_No.iloc[0]
    sm = df.Section_Master.iloc[0]

    def get_items_in_ns(df):
        df = df.sort_values(["sales_diff", "Item_Count"], ascending=[True, True])
        return {
            "Need_State": df.Need_State.iloc[0],
            "Items": [
                (item_no, sales_diff, facings)
                for (item_no, sales_diff), facings in zip(
                    zip(df.Item_No, df.sales_diff), df.Optim_Facings
                )
            ],
        }

    all_items_ns = df.groupby("Need_State").apply(get_items_in_ns)
    all_items_ns = {e["Need_State"]: e["Items"] for e in all_items_ns}

    applicable_need_states_final = set(all_items_ns.keys())
    applicable_need_states = applicable_need_states_final.copy()
    item_orders = []
    while all_items_ns:
        while applicable_need_states:
            ns_to_remove, item_to_remove, facings, min_diff = get_min(
                all_items_ns, applicable_need_states
            )
            if (
                len(all_items_ns[ns_to_remove]) == 1 and facings == 1
            ):  # NS is protected - remove from consideration.
                del all_items_ns[ns_to_remove]
                applicable_need_states_final.remove(ns_to_remove)
                applicable_need_states.remove(ns_to_remove)
            else:  # NS is not protected
                item_orders.append(item_to_remove)  # add item to removal list
                applicable_need_states.remove(
                    ns_to_remove
                )  # remove NS from round robin this round
                all_items_ns[ns_to_remove].pop(
                    0
                )  # remove item from consideration permanently
                if not all_items_ns[ns_to_remove]:  # remove section if NS is exhausted
                    del all_items_ns[ns_to_remove]
                    applicable_need_states_final.remove(ns_to_remove)
        applicable_need_states = applicable_need_states_final.copy()
    return pd.DataFrame(
        {
            "ITEM_TO_REMOVE": pd.Series(item_orders).astype(int),
            "ORDER": range(1, len(item_orders) + 1),
        }
    )


def store_section_facing_add_space_to_low_supplier_percentages(
    df: pd.DataFrame, space_analysis: pd.DataFrame, mapping: pd.DataFrame
) -> pd.DataFrame:
    """This function adds space to supplier Own brands items that didn't reveive enough space.
    The function merges the request and item mapping onto the item output and then iterates adding facings until
    no more space is left.

    Parameters
    ----------
    df: pd.DataFrame
        df of item output coming out of the item_output level function, unit proportions, or
        supplier adjustment with current and optimal facings and sales/margin
    space_analysis: pd.DataFrame
            supplier_own_brands_analysis: pd.DataFrame
        df with space analysis of total space, percentage requested of space and actual percentage post unit props
    mapping: pd.DataFrame
        df with mapping of items for a particular supplier own brands ID

    Returns
    ----------
    item_output: pd.DataFrame
        df of item output with facings added for those items where supplier ob was allocated not enough after unit
        proportions
    space_added: float
        float to return which then is used in the item removal function for items of supplier which received more
        than requested or other items
    """
    n = get_col_names()

    item_cols = list(df.columns)

    # calculate total space to add if there are more than 1 supplier requests that need addition
    gap_to_alloc = space_analysis.loc[space_analysis[n.F_SPACE_TO_ADD] > 0][
        n.F_SPACE_TO_ADD
    ].sum()

    # extend supple request + mapping to get to item matching
    request_items = space_analysis.merge(
        mapping[[n.F_ITEM_NO, n.F_SUPPLIER_OB_ID]], on=[n.F_SUPPLIER_OB_ID], how="left"
    )
    # merge space to add on item numbers
    df = df.merge(
        request_items[[n.F_ITEM_NO, n.F_SPACE_TO_ADD]], on=[n.F_ITEM_NO], how="left"
    )

    # determine which items are adding facings based on unit proportions for those under supplier OBs
    # determine proportions for only subset of items that need space
    supplier_ob_proportions = df.loc[df[n.F_SPACE_TO_ADD] > 0]
    supplier_ob_proportions["Unit_Prop"] = (
        supplier_ob_proportions[n.F_ITEM_COUNT]
        / supplier_ob_proportions[n.F_ITEM_COUNT].sum()
    )

    # add those facings within section for supplier OB
    # calculate new facings from unit proportion for items not treated constant
    supplier_ob_proportions = iterate_additional_space_by_item_proportion(
        n=n,
        df_with_ns_totals=supplier_ob_proportions,
        output_col=n.F_OPT_FACINGS,
        gap_to_alloc=gap_to_alloc,
        dont_go_over=False,
        input_col="Unit_Prop",
    )

    # check whether we exceeded max facings per section and limit assignment
    # calculate amount to subtract from 'add facings' if we are over max facings
    supplier_ob_proportions["limit_subtract"] = np.where(
        supplier_ob_proportions[n.F_OPT_FACINGS]
        > supplier_ob_proportions[n.F_MAX_FACINGS_IN_OPT],
        supplier_ob_proportions[n.F_OPT_FACINGS]
        - supplier_ob_proportions[n.F_MAX_FACINGS_IN_OPT],
        0,
    )
    # now update opt facings if we are over to max facings amount
    supplier_ob_proportions[n.F_OPT_FACINGS] = np.where(
        supplier_ob_proportions[n.F_OPT_FACINGS]
        > supplier_ob_proportions[n.F_MAX_FACINGS_IN_OPT],
        supplier_ob_proportions[n.F_MAX_FACINGS_IN_OPT],
        supplier_ob_proportions[n.F_OPT_FACINGS],
    )
    # now subtract limit (which is positive if over) from add facings to calculate correct 'space added'
    supplier_ob_proportions["Add_facings"] = (
        supplier_ob_proportions["Add_facings"]
        - supplier_ob_proportions["limit_subtract"]
    )
    supplier_ob_proportions = supplier_ob_proportions.drop("limit_subtract", axis=1)

    # update opt width x facings
    supplier_ob_proportions[n.F_OPT_WIDTH_X_FAC] = (
        supplier_ob_proportions[n.F_WIDTH_IN] * supplier_ob_proportions[n.F_OPT_FACINGS]
    )

    space_added = (
        supplier_ob_proportions["Add_facings"] * supplier_ob_proportions[n.F_WIDTH_IN]
    )

    # retain original columns
    item_cols.append(n.F_SPACE_TO_ADD)
    supplier_ob_proportions = supplier_ob_proportions[item_cols]
    # remove original assignments from df (item outputs) and replace by suppler ob proportions which has added facings
    df = df.loc[~(df[n.F_ITEM_NO].isin(supplier_ob_proportions[n.F_ITEM_NO].to_list()))]
    df = pd.concat([df, supplier_ob_proportions], axis=0)

    # flag which items are originally over for which we remove facings in next function
    df["removable"] = np.where(~(df[n.F_SPACE_TO_ADD] > 0), 1, 0)
    df = df.drop([n.F_SPACE_TO_ADD], axis=1)

    # remove max facings column - no longer needed
    df = df.drop(n.F_MAX_FACINGS_IN_OPT, axis=1)

    return df, space_added.sum()


def iterate_additional_space_by_item_proportion(
    n,
    df_with_ns_totals,
    output_col,
    gap_to_alloc,
    dont_go_over=True,
    input_col="Unit_Prop",
):
    """
    The function merges the request and item mapping onto the item output and then iterates adding facings until
    no more space is left.
    This function allocate additional space to items with the following logics:
    1: It primarily allocate the space to the items that are proportionally with the items amount.
       Which means, if the items got sold more. It will be allocated more facings.
    2: For the upcoming items, it will try to allocate at least 1 facing for each item, until there is no more space.

    Parameters
    ----------
    n: dict,
        this is the names dict that includes all column names and rename lists / dictionaries
    df_with_ns_totals: pd.DataFrame
        df of item output coming out of the item_output level function, unit proportions, or
        supplier adjustment with current and optimal facings and sales/margin
    output_col: str
        column name of output (either facings or opt facings)
    gap_to_alloc: int
        the amount of space that are free to allocate
    dont_go_over: bool (Default: True)
        For unit proportions code after optimization we don't want to go over space so should be True. For
        supplier request additions post unit proportions we want at least that requested space so this should be False
    input_col: str
        column name for Item Count Proportion (Default: Unit_Prop)

    Returns
    ----------
    df_with_ns_totals: pd.DataFrame
        df of item output with facings added for those items where supplier ob was allocated not enough after unit
        proportions
    """

    # Temporary New Optimum Facing
    tem_col_name = "Add_facings"
    # Sort the df based on item_count_prop and select only the non-constant rows
    df_with_ns_totals = df_with_ns_totals.sort_values(input_col, ascending=False)
    df_non_constant = df_with_ns_totals[df_with_ns_totals[n.F_CONST_TREAT] == False]
    origin_gap_to_alloc = gap_to_alloc
    df_non_constant[tem_col_name] = 0

    # Iterate the dataframe
    # 1: If the item width is less than the space_to_alloc, allocate the width that is proportional to item's amount
    # 2: If the item width is the first one bigger than the space to alloc, add 1 to this item's opt facing
    # 3: Otherwise, keep the item's opt facing unchanged

    # Attention: For step 1, add at least 1 if there is space left and this item fits.
    #            The logic is items are sorted by proportion. So the above items are more valuable than below.
    #            And if we could add 1 for the above items. Most likely it would not cost more space compare to
    #            add 1 for the below items.
    for index, row in df_non_constant.iterrows():

        if row[n.F_WIDTH_IN] <= gap_to_alloc and gap_to_alloc > 0:
            items_counter = np.ceil(
                (origin_gap_to_alloc * row[input_col]) / (row[n.F_WIDTH_IN])
            )

            # Check the ceiled items_counter pass the gap_to_alloc or not
            if items_counter * row[n.F_WIDTH_IN] > gap_to_alloc:
                items_counter -= 1

            df_non_constant.loc[index, tem_col_name] = items_counter
            gap_to_alloc = gap_to_alloc - row[n.F_WIDTH_IN] * items_counter

        elif row[n.F_WIDTH_IN] > gap_to_alloc > 0 and not dont_go_over:
            df_non_constant.loc[index, tem_col_name] = 1
            gap_to_alloc = -1

        else:
            df_non_constant.loc[index, tem_col_name] = 0

    # Merge the New_Opt_Facings with (Original) Opt_Facings
    df_with_ns_totals = df_with_ns_totals.merge(
        df_non_constant[[n.F_ITEM_NO, tem_col_name]], on=[n.F_ITEM_NO], how="left"
    )
    df_with_ns_totals[output_col] = (
        df_with_ns_totals[tem_col_name] + df_with_ns_totals[n.F_OPT_FACINGS]
    )

    return df_with_ns_totals


def quality_assurance_check(df: pd.DataFrame) -> None:
    n = get_col_names()

    keys = [
        n.F_REGION_DESC,
        n.F_BANNER,
        n.F_DEPARTMENT,
        n.F_M_CLUSTER,
        n.F_STORE_PHYS_NO,
        n.F_ITEM_NO,
    ]
    deduped = df.drop_duplicates(subset=keys)

    msg = "Duplicate items per region banner department cluster store detected"
    assert len(df) == len(deduped), msg


def select_cluster_df(
    df_clusters_external: SparkDataFrame,
    df_clusters_auto: SparkDataFrame,
    use_revised_merged_clusters: bool,
):
    """
    Selects the correct merged cluster table, depending on whether we use the revised merged clusters.

    Parameters
    ----------
    df_clusters_external : manually reviewed merge cluster table
    df_clusters_auto : automatically concatenated merge cluster table from pipeline
    use_revised_merged_clusters : whether to use the manually reviewed version

    Returns
    -------
    df_clusters : SparkDataFrame
      Correct merged cluster lookup table to use
    """
    if use_revised_merged_clusters:
        df_clusters = df_clusters_external
        df_clusters = df_clusters.withColumnRenamed("MERGE_CLUSTER", "M_Cluster")
    else:
        df_clusters = df_clusters_auto.withColumnRenamed("MERGED_CLUSTER", "M_Cluster")
    df_clusters = df_clusters.select("STORE_PHYSICAL_LOCATION_NO", "M_Cluster")
    return df_clusters


def read_and_process_item_counts(
    df_bay_data: SparkDataFrame,
    item_output: SparkDataFrame,
    cluster_assignment: SparkDataFrame,
):

    # get column names
    n = get_col_names()

    bay_data = df_bay_data.select(
        "NATIONAL_BANNER_DESC",
        "REGION",
        "SECTION_MASTER",
        "STORE_PHYSICAL_LOCATION_NO",
        n.F_ITEM_NO,
        "SALES",
        n.F_FACINGS,
        "E2E_MARGIN_TOTAL",
        "NEED_STATE",
        "ITEM_COUNT",
    )

    # in case we have stores with multiple facings over time represented in the df, we don't
    # want to duplciate the item counts so we drop dupes accross facings
    bay_data = bay_data.dropDuplicates(
        subset=["NEED_STATE", "STORE_PHYSICAL_LOCATION_NO", n.F_ITEM_NO]
    )
    bay_data = bay_data.withColumnRenamed(
        "National_Banner_Desc", n.F_BANNER
    ).withColumnRenamed("Region", n.F_REGION_DESC)

    # filter based on config region, banner, dept
    lower = F.lower(F.col(n.F_REGION_DESC))
    upper = F.upper(F.col(n.F_REGION_DESC))
    initcap = F.initcap(F.col(n.F_REGION_DESC))

    # TODO not needed because we join directly with missing items
    mask = lower.isin(*region_list) | (
        upper.isin(*region_list) | (initcap.isin(*region_list))
    )
    # filter POG on region and banner list
    bay_data = bay_data.filter(mask)

    bay_data = bay_data.filter(F.col(n.F_BANNER).isin(*banner_list))

    # filter department
    # merge in department onto section master to filter on it
    # TODO not needed because we join direclty in
    POG_department_mapping = spark.createDataFrame(pdf_pog_department_mapping)
    POG_department_mapping = POG_department_mapping.select(
        n.F_REGION_DESC, n.F_BANNER, n.F_SECTION_MASTER, n.F_DEPARTMENT
    )
    # TODO not needed because we join direclty in
    POG_department_mapping = POG_department_mapping.filter(
        F.col(n.F_DEPARTMENT).isin(*depts)
    )

    bay_data = bay_data.join(
        POG_department_mapping,
        on=[n.F_REGION_DESC, n.F_BANNER, n.F_SECTION_MASTER],
        how="inner",
    )

    # TODO convert to SPARK all below
    # drop any duplicates
    bay_data = bay_data.drop_duplicates(
        subset=[
            n.F_NEED_STATE,
            n.F_SECTION_MASTER,
            n.F_STORE_PHYS_NO,
            n.F_ITEM_NO,
            n.F_FACINGS,
        ]
    )

    # aggregate sales or margin across all facings now per store
    keys = [
        n.F_REGION_DESC,
        n.F_SECTION_MASTER,
        n.F_NEED_STATE,
        n.F_STORE_PHYS_NO,
        n.F_ITEM_NO,
    ]
    summary = bay_data.pivot_table(
        index=keys, values=n.F_ITEM_COUNT, aggfunc=np.sum
    ).reset_index()

    # merge store cluster in so we can aggregate on it
    summary = summary.merge(cluster_assignment, on=[n.F_STORE_PHYS_NO], how="left")

    # aggregate avg. sales across all stores now
    keys_avg = [n.F_M_CLUSTER, n.F_NEED_STATE, n.F_ITEM_NO]
    summary_avg = summary.pivot_table(
        index=keys_avg, values=n.F_ITEM_COUNT, aggfunc=np.mean
    ).reset_index()

    # now we merge historic sales or margin into the item output
    # summary_avg.select(ITEM COUNT)
    item_output_new = item_output.merge(summary_avg, on=keys_avg, how="left")

    return item_output_new


def get_projected_sales_margin(
    df_opt: SparkDataFrame,
    df_clusters: SparkDataFrame,
    elas_sales: SparkDataFrame,
    elas_margins: SparkDataFrame,
):
    """
    Determine the projected sales and marign by looking up the optimal facings in the elasticity tables

    Parameters
    ----------
    df_opt : optimization outputs, at item level
    df_clusters : merged cluster lookup table
    elas_sales : sales elasticity curves
    elas_margins : margin elasticity curves

    Returns
    -------
    pdf_opt_agg : pd.DataFrame
      Optimizatoin outputs at the store-section level with projected sales + margin
    """
    df_opt = df_opt.join(df_clusters, on=["Store_Physical_Location_No"])
    df_opt = df_opt.withColumnRenamed("Region_Desc", "REGION")
    elas_sales = elas_sales.withColumnRenamed("National_Banner_Desc", "BANNER")
    elas_margins = elas_margins.withColumnRenamed("National_Banner_Desc", "BANNER")

    opt_sales = df_opt.join(
        elas_sales, on=["REGION", "BANNER", "M_cluster", "SECTION_MASTER", "ITEM_NO"]
    )
    opt_margins = df_opt.join(
        elas_margins, on=["REGION", "BANNER", "M_cluster", "SECTION_MASTER", "ITEM_NO"]
    )

    opt_sales = opt_sales.withColumn(
        "facing_fit_list", F.array(*[F.col("facing_fit_" + str(i)) for i in range(13)])
    )
    opt_sales = opt_sales.drop(*["facing_fit_" + str(i) for i in range(13)])
    opt_sales = opt_sales.withColumn(
        "optim_sales",
        opt_sales.facing_fit_list.getItem(F.col("Optim_Facings").cast(T.IntegerType())),
    )
    opt_sales = opt_sales.withColumn(
        "curr_sales",
        opt_sales.facing_fit_list.getItem(
            F.col("Current_Facings").cast(T.IntegerType())
        ),
    )

    opt_margins = opt_margins.withColumn(
        "facing_fit_list", F.array(*[F.col("facing_fit_" + str(i)) for i in range(13)])
    )
    opt_margins = opt_margins.drop(*["facing_fit_" + str(i) for i in range(13)])
    opt_margins = opt_margins.withColumn(
        "optim_margins",
        opt_margins.facing_fit_list.getItem(
            F.col("Optim_Facings").cast(T.IntegerType())
        ),
    )
    opt_margins = opt_margins.withColumn(
        "curr_margins",
        opt_margins.facing_fit_list.getItem(
            F.col("Current_Facings").cast(T.IntegerType())
        ),
    )

    opt_sales_margins = opt_sales.join(
        opt_margins.select(
            "Store_Physical_Location_No", "Item_No", "optim_margins", "curr_margins"
        ),
        on=["Store_Physical_Location_No", "Item_No"],
    )

    opt_agg = opt_sales_margins.groupBy(
        "Region", "BANNER", "Store_Physical_Location_No", "Department", "Section_Master"
    ).agg(
        F.sum("curr_margins").alias("SUM_CURR_MARGINS"),
        F.sum("curr_sales").alias("SUM_CURR_SALES"),
        F.sum("optim_margins").alias("SUM_OPT_MARGINS"),
        F.sum("optim_sales").alias("SUM_OPT_SALES"),
        F.sum("Cur_Width_X_Facing").alias("SUM_CURR_WIDTH"),
        F.sum("Opt_Width_X_Facing").alias("SUM_OPT_WIDTH"),
    )

    pdf_opt_agg = opt_agg.toPandas()
    return pdf_opt_agg


def filter_bad_pogs(
    pdf: pd.DataFrame, pdf_deviations: pd.DataFrame, pog_filter_cutoff: float
):
    """
    For sales/margin analysis, filter out sections that are invalid due to large discrepancies in sales and POG data.

    Parameters
    ----------
    pdf : table at store-section level with sales/margin data
    pdf_deviations : reference table of sections' width and sales deviations
    pog_filter_cutoff : threshold to determine whether a POG is valid

    Returns
    -------
    good_pdf : pd.DataFrame
      Projected sales only for valid store-sections
    """
    deviation_cutoff = 1.0 - pog_filter_cutoff
    good_pogs = pdf_deviations[
        (
            pdf_deviations["Cur_Section_Width_Deviation"].astype(float)
            <= deviation_cutoff
        )
        & (
            pdf_deviations["Cur_Section_Width_Deviation"].astype(float)
            >= -1 * deviation_cutoff
        )
        & (pdf_deviations["Section_Perc_W_Sales"].astype(float) >= pog_filter_cutoff)
    ]

    good_pogs = good_pogs.drop_duplicates(
        ["Store_Physical_Location_No", "Section_Master"]
    )

    good_pdf = pdf.merge(
        good_pogs[["Store_Physical_Location_No", "Section_Master"]],
        on=["Store_Physical_Location_No", "Section_Master"],
    )
    return good_pdf


def combine_pre_post_reruns(pre_margin: pd.DataFrame, post_margin: pd.DataFrame):
    """
    Combine the pre margin-rerun and margin-rerun tables (by adding additional cols) for sales/margin projections

    Parameters
    ----------
    pre_margin : results from pre-margin rerun
    post_margin : results after margin rerun

    Returns
    -------
    combined : pd.DataFrame
      combined table with cols of results both from pre-margin rerun and post-margin rerun
    """
    post_margin = post_margin.set_index(
        ["Store_Physical_Location_No", "Section_Master"]
    )
    pre_margin = pre_margin.set_index(["Store_Physical_Location_No", "Section_Master"])

    post_margin_full = pre_margin.copy()
    post_margin_full.update(post_margin)

    post_margin_full = post_margin_full[
        ["SUM_OPT_MARGINS", "SUM_OPT_SALES", "SUM_OPT_WIDTH"]
    ]
    post_margin_full = post_margin_full.rename(
        {col: col + "_POST_MARGIN_RERUN" for col in post_margin_full.columns}, axis=1
    )
    combined = pd.concat([pre_margin, post_margin_full], axis=1)
    combined = combined.reset_index()
    return combined


def get_margin_rerun_sales(
    df_opt_rerun: SparkDataFrame,
    df_elas_sales: SparkDataFrame,
    df_clusters: SparkDataFrame,
):
    """
    Get new projected sales of items with updated facings after margin rerun

    Parameters
    ----------
    df_opt_rerun : margin rerun outputs
    df_elas_sales : sales elasticity curves
    df_clusters : merged clusters lookup

    Returns
    -------
    opt_sales : SparkDataFrame
      margin rerun outputs with sales projections
    """
    df_opt_rerun = df_opt_rerun.join(df_clusters, on=["Store_Physical_Location_No"])
    df_opt_rerun = df_opt_rerun.withColumnRenamed("Region_Desc", "REGION")
    df_elas_sales = df_elas_sales.withColumnRenamed("National_Banner_Desc", "BANNER")

    opt_sales = df_opt_rerun.join(
        df_elas_sales, on=["REGION", "BANNER", "M_cluster", "SECTION_MASTER", "ITEM_NO"]
    )
    opt_sales = opt_sales.withColumn(
        "facing_fit_list", F.array(*[F.col("facing_fit_" + str(i)) for i in range(13)])
    )
    opt_sales = opt_sales.drop(*["facing_fit_" + str(i) for i in range(13)])
    opt_sales = opt_sales.withColumn(
        "Opt_Sales",
        opt_sales.facing_fit_list.getItem(F.col("Optim_Facings").cast(T.IntegerType())),
    )
    return opt_sales


def update_margin_facings(df_opt: SparkDataFrame, df_opt_rerun: SparkDataFrame):
    """
    Update pre-margin rerun table with post-margin rerun results for select cluster-sections that were rerun

    Parameters
    ----------
    df_opt : pre-margin rerun outputs
    df_opt_rerun : post-margin rerun outputs

    Returns
    -------
    df_opt : SparkDataFrame
      opt outputs that now reflect changes made in margin rerun
    """
    overwrite_cols = [
        "Optim_Facings",
        "Unique_Items_Opt_Assorted",
        "Opt_Width_X_Facing",
        "Cur_Opt_Same_Facing",
        "Opt_Minus_Cur_Facings",
        "Opt_Sales",
    ]
    df_opt_rerun = df_opt_rerun.select(
        "Store_Physical_Location_No", "Item_No", *overwrite_cols
    )
    for col in overwrite_cols:
        df_opt_rerun = df_opt_rerun.withColumnRenamed(col, col + "_rerun")
    df_opt = df_opt.join(
        df_opt_rerun, on=["Store_Physical_Location_No", "Item_No"], how="left"
    )
    for col in overwrite_cols:
        df_opt = df_opt.withColumn(
            col,
            F.when(df_opt[col + "_rerun"].isNull(), F.col(col)).otherwise(
                F.col(col + "_rerun")
            ),
        )
        df_opt = df_opt.drop(col + "_rerun")

    return df_opt


def get_margin_rerun_margins(
    df_opt_rerun: SparkDataFrame,
    df_elas_margins: SparkDataFrame,
    df_clusters: SparkDataFrame,
):
    """
    Get projected margins of items in opt outputs

    Parameters
    ----------
    df_opt_rerun : opt outputs
    df_elas_margins : margin elasticity curves
    df_clusters : merged clusters lookup

    Returns
    -------
    opt_margins : SparkDataFrame
      opt outputs with margin projections
    """
    df_opt_rerun = df_opt_rerun.join(
        df_clusters, on=["Store_Physical_Location_No", "M_Cluster"]
    )
    df_opt_rerun = df_opt_rerun.withColumnRenamed("Region_Desc", "REGION")
    df_elas_margins = df_elas_margins.withColumnRenamed(
        "National_Banner_Desc", "BANNER"
    )
    opt_margins = df_opt_rerun.join(
        df_elas_margins.select(
            "REGION",
            "BANNER",
            "M_Cluster",
            "SECTION_MASTER",
            "ITEM_NO",
            *[F.col("facing_fit_" + str(i)) for i in range(13)],
        ),
        on=["REGION", "BANNER", "M_Cluster", "SECTION_MASTER", "ITEM_NO"],
    )

    opt_margins = opt_margins.withColumn(
        "facing_fit_list", F.array(*[F.col("facing_fit_" + str(i)) for i in range(13)])
    )
    opt_margins = opt_margins.drop(*["facing_fit_" + str(i) for i in range(13)])
    opt_margins = opt_margins.withColumn(
        "Opt_Margin",
        opt_margins.facing_fit_list.getItem(
            F.col("Optim_Facings").cast(T.IntegerType())
        ),
    )
    opt_margins = opt_margins.withColumn(
        "Cur_Margin",
        opt_margins.facing_fit_list.getItem(
            F.col("Current_Facings").cast(T.IntegerType())
        ),
    )

    # need to drop M_Cluster again
    opt_margins = opt_margins.drop("facing_fit_list")

    return opt_margins


def create_msg_for_udf_run_scope(pdf: pd.DataFrame) -> str:
    """
    create message for logging scope for running the UDF in a format that
    can be copied & pasted into console to quickly set the scope
    """

    pdf = pdf.drop_duplicates()

    assert len(pdf) == 1, "pdf must have cardinality of 1 on all columns"
    scope = pdf.to_dict("records")[0]
    msg_scope = "; ".join([f"{k.lower()}={repr(v)}" for k, v in scope.items()])

    return msg_scope


def create_exec_id_for_udf_run_scope(pdf: pd.DataFrame) -> str:
    """
    create a unique ID out of the single-row pdf representing the scope
    that will be given to opt UDF.
    For e.g. this exec ID can be used for tracking runs on Gurobi dashboard
    """

    pdf = pdf.drop_duplicates()

    assert len(pdf) == 1, "pdf must have cardinality of 1 on all columns"
    scope = pdf.to_dict("records")[0]
    exec_id = "_".join([f"{v}" for _, v in scope.items()]).replace(" ", "")

    # NOTE: this ID cannot contain spaces as it will break Pulp<->Gurobi
    # interface
    return exec_id


def add_region_banner_dept_store_to_pdf(
    pdf: pd.DataFrame,
    region: str,
    banner: str,
    dept: str,
    store: str,
    dataset_name: str,
    if_exists: Optional[str] = "break",
    keep_suffix: Optional[str] = "original",
):
    """
    Adds dimensional columns to a pd dataframe
    NOTE: if this breaks ensure your data does not carry dimensional
    data that is being added here

    Parameters
    ----------
    pdf : pd.DataFrame
        pandas dataframe to add dimensional columns to
    region : str
        dimensional data for 'region'
    banner : str
        dimensional data for 'banner'
    dept : str
        dimensional data for 'dept'
    store : str
        dimensional data for 'store'

    dataset_name: str
        a friendly tag to add to error message for ease of debugging

    if_exists: str
        what to do if a particular dimension already exists in the data

    keep_suffix: str
        if if_exists=="keep" what is the suffix to be added to the ORIGINAL
        column name

    Returns
    -------
    pandas dataframe with  dimensional columns
    """

    # mapping of col names to values
    # Q: why does it have "_DIM" at the end?
    # A: to minimize chance that the name conflicts with any existing names
    dict_col_val_mapping = {
        "REGION_DIM": region,
        "BANNER_DIM": banner,
        "DEPT_DIM": dept,
        "STORE_DIM": store,
    }

    cols_orig = list(pdf.columns)
    cols_existing = [x.lower() for x in cols_orig]

    for col_name, col_val in dict_col_val_mapping.items():

        is_exists = col_name.lower() in cols_existing

        if not is_exists:
            pdf[col_name] = col_val
            continue

        # here we know that the column already exists
        # get the CURRENT name of the column
        col_cur = [x for x in cols_orig if x.lower() == col_name.lower()][0]

        if if_exists == "break":
            msg = f"""
            In the following dataset: '{dataset_name}'
            Column '{col_name}' already exists in the data:
            \n{pdf}\n
            """

            raise Exception(msg)

        elif if_exists == "drop":
            pdf = pdf.drop([col_cur], axis=1, inplace=False)
            pdf[col_name] = col_val

        elif if_exists == "ignore":
            continue

        elif if_exists == "rename":
            ren = {col_cur: col_name}
            pdf = pdf.rename(ren, axis=1, errors="raise", inplace=False)

        elif if_exists == "keep":
            col_cur_new = f"{col_cur}_{keep_suffix}"
            msg = f"Column name with suffix is same as column name without"
            assert col_cur_new != col_cur, msg
            msg = f"We want to keep col '{col_cur_new}' but it already exists"
            assert not col_cur_new.lower() in cols_existing, msg
            ren = {col_cur: col_cur_new}
            pdf = pdf.rename(ren, axis=1, errors="raise", inplace=False)
            pdf[col_name] = col_val

        else:
            raise Exception(f"Invalid value for 'if_exists' argument: '{if_exists}'")

    return pdf


def generate_region_banner_dept_store_skeleton(
    df_task_opt_modelling_create_and_solve_model: SparkDataFrame,
    col_name_with_results: Optional[str] = None,
    exclude_errors: Optional[bool] = False,
) -> SparkDataFrame:
    """generates distinct set of main opt dimensions"""
    from spaceprod.utils.space_context.spark import spark

    df = df_task_opt_modelling_create_and_solve_model

    if exclude_errors:
        msg = f"Must pass 'col_name_with_results' if excluding errors"
        assert col_name_with_results is not None, msg

        # exclude errors
        mask_error = get_opt_error_mask()
        df = df.filter(~mask_error)

    cols = ["REGION", "BANNER", "DEPT", "STORE"]
    df_skeleton = df.select(*cols).dropDuplicates()
    df_skeleton = backup_on_blob(spark, df_skeleton)
    return df_skeleton


def post_process_concatinated_elasticity_data(
    df_elasticity_curves: SparkDataFrame,
    rerun: bool,
    dep_var: str,
) -> SparkDataFrame:
    """
    performs de-duping of elasticity curve data to get rid of store dimension
    TODO: evaluate if similar procedure needs to be done with rest of the
     concated datasets
    """

    if rerun:
        msg = "You are running 'rerun==True, 'dep_var' must be 'Margin' "
        assert dep_var == "Margin", msg
    else:
        msg = "You are running 'rerun==False, 'dep_var' must be 'Sales' "
        assert dep_var == "Sales", msg

    cols = [
        "Section_Master",
        "Need_State",
        "Need_State_Idx",
        "Item_No",
        "Item_Name",
        "M_Cluster",
        "Facings",
        dep_var,
        "REGION_DIM",
        "BANNER_DIM",
        "DEPT_DIM",
    ]

    df = df_elasticity_curves.select(*cols).dropDuplicates()
    return df


def update_margin_breaks(df_opt, df_opt_rerun):
    df_opt_rerun_breaks = df_opt_rerun.select(
        "Store_Physical_Location_No", "Section_Master", "Opt_Legal_Breaks"
    )
    df_opt_rerun_breaks = df_opt_rerun_breaks.withColumnRenamed(
        "Opt_Legal_Breaks", "Opt_Legal_Breaks_Margin"
    )

    df_opt_updated = df_opt.join(
        df_opt_rerun_breaks,
        on=["Store_Physical_Location_No", "Section_Master"],
        how="left",
    )
    df_opt_updated = df_opt_updated.withColumn(
        "Opt_Legal_Breaks",
        F.when(
            ~F.col("Opt_Legal_Breaks_Margin").isNull(), F.col("Opt_Legal_Breaks_Margin")
        ).otherwise(F.col("Opt_Legal_Breaks")),
    )
    df_opt_updated = df_opt_updated.drop("Opt_Legal_Breaks_Margin")

    # add opt theoretic
    df_opt_updated = df_opt_updated.withColumn(
        "Opt_Theoretic_Lin_Space",
        F.col("Opt_Legal_Breaks") * F.col("Lin_Space_Per_Break"),
    )
    return df_opt_updated
