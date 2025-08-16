#########################################################################################
# helpers.py
# Contains all the QA checks that will be run by QA module
#########################################################################################
from typing import Tuple

import pandas as pd
from spaceprod.src.optimization.checks.helpers_dataframe import *
from spaceprod.src.optimization.checks.helpers_plot_module import (
    plot_cluster_scatter_facing_pen_vs_sales_pen,
)
from spaceprod.src.optimization.checks.sense_check_results import (
    SenseCheckResult,
)
from spaceprod.utils.names import get_col_names
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.imports import F


def sense_check_uplift_by_section(
    pdf_item_master,
    pdf_margin_section,
    dependent_var,
    tol_perc,
    space_tol_perc,
    test_id: str,
    sheet_name: str,
) -> Tuple[SenseCheckResult, SenseCheckResult]:
    f"""This QA function checks the margin and sales uplift by section level, and will only report sales for
    non-margin section, and margins for margin section.

    Parameters
    ----------
    pdf_item_master: pd.DataFrame
        this is the dataframe that has the optimization result per item for each store
    pdf_margin_section: pd.DataFrame
        this is the dataframe that has all the margin sections (The sections will be optimized over margin)
    dependent_var: string
        this is the variable identify which uplift you want to calculate, has to be one of ["Sales", "Margin"]
    tol_perc: float
        this is the tolerance percentage for message report. In short, the final message will show how many perc
        stores has dependent_var decrease percentage more than tol_perc
    space_tol_perc: float
        this is similar for tol_perc but for facing changes.
    test_id: str
        a string representing an ID (order number) in the summary view
    sheet_name: str
        sheet name in excel that would contain dataset containing the results

    Returns
    -------
    result_uplift: SenseCheckResult
        The SenseCheckResult contains the uplift report dataframe and the report message of the `dependent_var`.
    result_facings: SenseCheckResult
        The SenseCheckResult contains the uplift report dataframe and the report message of the `dependent_var`'s facings.
    """
    n = get_col_names()
    # Make a copy of the original dataset to avoid change on the original dataset
    pdf_item_master = pdf_item_master.copy()

    # Create the section level key as a list
    section_level_key = [
        n.F_STORE_PHYS_NO,
        n.F_REGION_DESC,
        n.F_BANNER,
        n.F_DEPARTMENT,
        n.F_SECTION_MASTER,
    ]

    if dependent_var not in ["Sales", "Margin"]:
        print(
            f"{dependent_var} is not one of [Sales, Margin], switch to Sales by default"
        )
        dependent_var = "Sales"

    # If the operation is sales, then we need a left excluding join
    # If it is a margin, then a simple join works
    if dependent_var == "Sales":
        pdf_store_section_master = pdf_item_master.merge(
            pdf_margin_section, indicator=True, how="left"
        )[lambda x: x._merge == "left_only"].drop("_merge", 1)
    elif dependent_var == "Margin":
        pdf_store_section_master = pdf_item_master.merge(
            pdf_margin_section, how="inner"
        )

    # Construct the table for process
    # 1. Group the items to the section level
    # 2. Add change and perc_change for both dependent variable and space (facings)
    pdf_store_section_uplift = (
        pdf_store_section_master[
            section_level_key
            + [
                f"Cur_{dependent_var}",
                f"Opt_{dependent_var}",
                n.F_CUR_FACINGS,
                n.F_OPT_FACINGS,
            ]
        ]
        .groupby(section_level_key)
        .sum()
        .reset_index()
    )

    pdf_store_section_uplift[f"{dependent_var}_Uplift"] = (
        pdf_store_section_uplift[f"Opt_{dependent_var}"]
        - pdf_store_section_uplift[f"Cur_{dependent_var}"]
    )

    pdf_store_section_uplift[f"{dependent_var}_Uplift_Perc"] = (
        pdf_store_section_uplift[f"{dependent_var}_Uplift"]
        / pdf_store_section_uplift[f"Cur_{dependent_var}"]
    )

    pdf_store_section_uplift["Facings_Uplift"] = (
        pdf_store_section_uplift[n.F_OPT_FACINGS]
        - pdf_store_section_uplift[n.F_CUR_FACINGS]
    )

    pdf_store_section_uplift["Facings_Uplift_Perc"] = (
        pdf_store_section_uplift["Facings_Uplift"]
        / pdf_store_section_uplift[n.F_CUR_FACINGS]
    )

    # The intermediate result tables for msg report
    pdf_decrease = pdf_store_section_uplift[
        pdf_store_section_uplift[f"{dependent_var}_Uplift"] < 0
    ]
    pdf_perc_decrease = pdf_store_section_uplift[
        pdf_store_section_uplift[f"{dependent_var}_Uplift_Perc"] < -tol_perc
    ]

    pdf_facing_decrease = pdf_store_section_uplift[
        pdf_store_section_uplift["Facings_Uplift"] < 0
    ]
    pdf_facing_perc_decrease = pdf_store_section_uplift[
        pdf_store_section_uplift["Facings_Uplift_Perc"] < -space_tol_perc
    ]

    decrease_perc = (
        pdf_decrease[section_level_key].shape[0]
        / pdf_store_section_uplift[section_level_key].shape[0]
    )

    perc_decrease_perc = (
        pdf_perc_decrease[section_level_key].shape[0]
        / pdf_store_section_uplift[section_level_key].shape[0]
    )

    facing_decrease_perc = (
        pdf_facing_decrease[section_level_key].shape[0]
        / pdf_store_section_uplift[section_level_key].shape[0]
    )

    facing_perc_decrease_perc = (
        pdf_facing_perc_decrease[section_level_key].shape[0]
        / pdf_store_section_uplift[section_level_key].shape[0]
    )

    # Create the append message to clarify the report message
    append_msgs = {"Sales": "Non-Margin", "Margin": "Margin"}

    msg = (
        f"Store Section: There are {convert_perc(decrease_perc)} of the {append_msgs[dependent_var]} "
        + f"store sections that have a decrease, and {convert_perc(perc_decrease_perc)} that have "
        + f"a more than {convert_perc(tol_perc)} decrease in {dependent_var}"
    )

    space_msg = (
        f"Store Section: There are {convert_perc(facing_decrease_perc)} of the {append_msgs[dependent_var]} "
        + f"store sections that have a decrease, and {convert_perc(facing_perc_decrease_perc)} that have "
        + f"a more than {convert_perc(space_tol_perc)} decrease in Facings"
    )

    pdf_store_section_uplift_rounded = round_qa_df(pdf_store_section_uplift)

    result_uplift = SenseCheckResult(
        pdf=pdf_store_section_uplift_rounded,
        message=msg,
        percentage=decrease_perc,
        percentage_target=0.05,
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}",
    )

    result_facings = SenseCheckResult(
        pdf=pdf_store_section_uplift_rounded,
        message=space_msg,
        percentage=facing_decrease_perc,
        percentage_target=0.05,
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}-space",
    )

    return result_uplift, result_facings


def sense_check_item_max_opt_facings(
    pdf_item_master,
    max_facings,
    test_id: str,
    sheet_name: str,
) -> SenseCheckResult:
    """This QA function checks every item that will have optimized facings less than maximum facings

    Parameters
    ----------
    pdf_item_master: pd.DataFrame
        this is the dataframe that has the optimization result per item (Opt_Margin and Cur_Margin)
    max_facings: int
        this is the maximum optim facings that will be passed from config
    test_id: str
        a string representing an ID (order number) in the summary view
    sheet_name: str
        sheet name in excel that would contain dataset containing the results

    Returns
    -------
    sense_check_result: SenseCheckResult
        The result of the item max optimum facings, which contains the report dataframe and report message
    """

    n = get_col_names()
    # The items that does not have a need state
    pdf_item_over_maximum = pdf_item_master[
        pdf_item_master[n.F_OPT_FACINGS] > max_facings
    ]

    # The summary message for summary file
    na_perc = len(pdf_item_over_maximum[n.F_ITEM_NO].unique()) / len(
        pdf_item_master[n.F_ITEM_NO].unique()
    )

    msg = f"Items: There are {convert_perc(na_perc)} of the items that has over (>) {max_facings} facings."

    pdf_item_over_maximum = pdf_item_over_maximum[
        [
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_STORE_PHYS_NO,
            n.F_DEPARTMENT,
            n.F_SECTION_MASTER,
            n.F_ITEM_NO,
            n.F_CUR_FACINGS,
            n.F_OPT_FACINGS,
        ]
    ]

    sense_check_result = SenseCheckResult(
        pdf=pdf_item_over_maximum,
        message=msg,
        percentage=na_perc,
        percentage_target=0,
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}",
    )

    return sense_check_result


def sense_check_need_state_assignment(
    pdf_item_master,
    test_id: str,
    sheet_name: str,
) -> SenseCheckResult:
    """This QA function checks every item has a need state

    Parameters
    ----------
    pdf_item_master: pd.DataFrame
        this is the dataframe that has the optimization result per item (Opt_Margin and Cur_Margin)
    test_id: str
        a string representing an ID (order number) in the summary view
    sheet_name: str
        sheet name in excel that would contain dataset containing the results

    Returns
    -------
    sense_check_result: SenseCheckResult
        The result of the need state assignment, which contains the report dataframe and report message
    """
    n = get_col_names()
    # The items that does not have a need state
    pdf_items_filtered_ns = pdf_item_master[pdf_item_master[n.F_NEED_STATE].isna()]

    # The summary message for summary file
    na_perc = len(pdf_items_filtered_ns[n.F_ITEM_NO].unique()) / len(
        pdf_item_master[n.F_ITEM_NO].unique()
    )

    msg = f"NeedState: There are {convert_perc(na_perc)} of the items that do not have a NeedState"

    pdf_item_wo_ns = pdf_items_filtered_ns[
        [
            n.F_STORE_PHYS_NO,
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_DEPARTMENT,
            n.F_SECTION_MASTER,
            n.F_ITEM_NO,
        ]
    ].assign(
        NeedState_Idx_IsNA=pdf_items_filtered_ns[n.F_NEED_STATE_IDX].isna(),
        NeedState_IsNA=pdf_items_filtered_ns[n.F_NEED_STATE].isna(),
    )

    sense_check_result = SenseCheckResult(
        pdf=pdf_item_wo_ns,
        message=msg,
        percentage=na_perc,
        percentage_target=0.01,
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}",
    )

    return sense_check_result


def sense_check_cluster_assignment(
    pdf_rb_dept_store_summary,
    test_id: str,
    sheet_name: str,
) -> SenseCheckResult:
    """This QA function checks every store has a cluster for each department

    Parameters
    ----------
    pdf_rb_dept_store_summary: pd.DataFrame
        this is the dataframe that has the optimization summary per store (M_Cluster)
    test_id: str
        a string representing an ID (order number) in the summary view
    sheet_name: str
        sheet name in excel that would contain dataset containing the results

    Returns
    -------
    sense_check_result: SenseCheckResult
        The result of the cluster assignment, which contains the report dataframe and report message
    """
    n = get_col_names()
    # The items that does not have a need state
    pdf_stores_filtered_cluster = pdf_rb_dept_store_summary[
        pdf_rb_dept_store_summary[n.F_M_CLUSTER].isna()
    ]

    filter_level = [n.F_REGION_DESC, n.F_BANNER, n.F_DEPARTMENT, n.F_STORE_PHYS_NO]

    # The summary message for summary file
    na_perc = (
        pdf_stores_filtered_cluster[filter_level].drop_duplicates().shape[0]
        / pdf_rb_dept_store_summary[filter_level].drop_duplicates().shape[0]
    )

    msg = f"Store: There are {convert_perc(na_perc)} of the stores' departments that do not belong to any clusters"

    pdf_store_wo_cluster = pdf_stores_filtered_cluster[filter_level].assign(
        Without_Cluster=True
    )

    sense_check_result = SenseCheckResult(
        pdf=pdf_store_wo_cluster,
        message=msg,
        percentage=na_perc,
        percentage_target=0.01,
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}",
    )

    return sense_check_result


def sense_check_need_state_facing(
    pdf_item_master,
    test_id: str,
    sheet_name: str,
) -> SenseCheckResult:
    """This QA function checks every need state need to have at least one facing for each store after opt

    Parameters
    ----------
    pdf_item_master: pd.DataFrame
        this is the dataframe that has the optimization result per item
    test_id: str
        a string representing an ID (order number) in the summary view
    sheet_name: str
        sheet name in excel that would contain dataset containing the results

    Returns
    -------
    sense_check_result: SenseCheckResult
        The result of the NeedState facings, which contains the report dataframe and report message
    """
    n = get_col_names()
    # Make a copy of the original dataset to avoid any change on the original dataset
    pdf_item_master = pdf_item_master.copy()
    # Get the top items to identify the needstate
    pdf_qa_result_top_x_items_per_ns_cur = summary_view_top_x_items_per_ns(
        pdf_item_master=pdf_item_master,
        dependent_var="Opt_Sales",
        within="Store",
        top_n=1,
        test_id="dummy",
        sheet_name="dummy",
    )

    pdf_top_items_per_ns = pdf_qa_result_top_x_items_per_ns_cur.pdf

    keys = [
        n.F_REGION_DESC,
        n.F_BANNER,
        n.F_STORE_PHYS_NO,
        n.F_DEPARTMENT,
        n.F_SECTION_MASTER,
        n.F_NEED_STATE,
    ]

    top_item = pdf_top_items_per_ns[keys + [n.F_ITEM_NAME]]

    # Construct the section level report table
    pdf_section_level_report_table = (
        pdf_item_master[keys + [n.F_OPT_FACINGS]]
        .groupby(keys)
        .sum()
        .reset_index()
        .merge(top_item, on=keys, how="left")
        .rename(columns={n.F_ITEM_NAME: "Top_Item_In_NS"})
    )

    # Construct the final message
    pdf_section_level_no_facing_table = pdf_section_level_report_table[
        pdf_section_level_report_table[n.F_OPT_FACINGS] <= 0
    ]
    na_perc = (
        pdf_section_level_no_facing_table[keys].shape[0]
        / pdf_section_level_report_table[keys].shape[0]
    )

    msg = f"NeedState: There are {convert_perc(na_perc)} of the NeedStates do not have at least one facing"

    sense_check_result = SenseCheckResult(
        pdf=pdf_section_level_report_table,
        message=msg,
        percentage=na_perc,
        percentage_target=0.01,
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}",
    )

    return sense_check_result


def sense_check_opt_categories_maximum_break_changes(
    pdf_legal_breaks,
    test_id: str,
    sheet_name: str,
) -> SenseCheckResult:
    """This function check whether each category per store will have more than 2 doors change.

    Parameters
    ----------
    pdf_legal_breaks: pd.DataFrame
        this is the dataframe that has the optimization result for breaks level (Cur_Legal_Breaks and Opt_Legal_Breaks)
    test_id: str
        a string representing an ID (order number) in the summary view
    sheet_name: str
        sheet name in excel that would contain dataset containing the results

    Returns
    -------
    sense_check_result: SenseCheckResult
        The result of the Maximum Break Changes for each Category, which contains the report dataframe and report message
    """
    n = get_col_names()
    # Make a copy of the data to avoid change on original dataset
    pdf_legal_breaks = pdf_legal_breaks.copy()

    pdf_legal_breaks["Legal_Break_Changes"] = (
        pdf_legal_breaks["Cur_Legal_Breaks"] - pdf_legal_breaks["Opt_Legal_Breaks"]
    )
    pdf_filtered_data = pdf_legal_breaks[
        pdf_legal_breaks["Legal_Break_Changes"].abs() > 2
    ]

    pdf_store_categories_break_change = pdf_filtered_data[
        [
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_DEPARTMENT,
            n.F_SECTION_MASTER,
            n.F_SECTION_NAME,
            n.F_STORE_PHYS_NO,
            "Opt_Legal_Breaks",
            "Cur_Legal_Breaks",
            "Legal_Break_Changes",
        ]
    ]

    na_perc = len(pdf_store_categories_break_change[n.F_STORE_PHYS_NO].unique()) / len(
        pdf_legal_breaks[n.F_STORE_PHYS_NO].unique()
    )

    msg = f"Store: There are {convert_perc(na_perc)} of the stores have more than 2 doors change in at least one section"

    sense_check_result = SenseCheckResult(
        pdf=pdf_store_categories_break_change,
        message=msg,
        percentage=na_perc,
        percentage_target=0.0,
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}",
    )
    return sense_check_result


def sense_check_opt_breaks_int(
    pdf_legal_breaks,
    test_id: str,
    sheet_name: str,
) -> SenseCheckResult:
    """This function check whether each category per store will have integer optimized breaks

    Parameters
    ----------
    pdf_legal_breaks: pd.DataFrame
        this is the dataframe that has the optimization result for breaks level (Cur_Legal_Breaks and Opt_Legal_Breaks)
    test_id: str
        a string representing an ID (order number) in the summary view
    sheet_name: str
        sheet name in excel that would contain dataset containing the results

    Returns
    -------
    sense_check_result: SenseCheckResult
        The result to check every break is integer, which contains the report dataframe and report message
    """
    n = get_col_names()
    # Make a copy of the dataset to avoid change on the original dataset
    pdf_legal_breaks = pdf_legal_breaks.copy()
    pdf_legal_breaks[
        "Is_Integer"
    ] = pdf_legal_breaks.Opt_Legal_Breaks == pdf_legal_breaks.Opt_Legal_Breaks.astype(
        int
    )
    pdf_legal_breaks_non_int = pdf_legal_breaks[pdf_legal_breaks["Is_Integer"] == False]

    pdf_store_breaks_summary = pdf_legal_breaks_non_int[
        [n.F_REGION_DESC, n.F_BANNER, n.F_STORE_PHYS_NO]
    ]

    na_perc = len(pdf_store_breaks_summary[n.F_STORE_PHYS_NO].unique()) / len(
        pdf_legal_breaks[n.F_STORE_PHYS_NO].unique()
    )

    msg = f"Store: There are {convert_perc(na_perc)} of the stores have non-integers optimized legal breaks in categories"

    sense_check_result = SenseCheckResult(
        pdf=pdf_store_breaks_summary,
        message=msg,
        percentage=na_perc,
        percentage_target=0.01,
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}",
    )

    return sense_check_result


def sense_check_assign_facing_by_unitprop(
    pdf_item_master,
    tol_perc,
    test_id: str,
    sheet_name: str,
) -> SenseCheckResult:
    """This function checks the facings assigned to each item are being assigned by unit proportion with in a threshhold

    Parameters
    ----------
    pdf_item_master: pd.DataFrame
        The item master dataframe that will be used to check on.
    tol_perc: float
        The tolerance percentage
    test_id: str
        a string representing an ID (order number) in the summary view
    sheet_name: str
        sheet name in excel that would contain dataset containing the results

    Returns
    -------
    sense_check_result: SenseCheckResult
        The result to check the facings are assigned by Unit Proportion, which contains the report dataframe and report message
    """
    n = get_col_names()
    # Create a copy of the dataframe to avoid change on the original dataset
    pdf_item_master = pdf_item_master.copy()

    main_level_keys = [
        n.F_REGION_DESC,
        n.F_BANNER,
        n.F_M_CLUSTER,
        n.F_STORE_PHYS_NO,
        n.F_SECTION_MASTER,
        n.F_NEED_STATE,
    ]

    pdf_item_master = pdf_item_master.dropna(subset=main_level_keys)
    pdf_item_master[n.F_ITEM_COUNT] = pdf_item_master[n.F_ITEM_COUNT].fillna(0)
    pdf_item_facing = pdf_item_master[
        main_level_keys + [n.F_ITEM_NO, n.F_OPT_FACINGS, n.F_ITEM_COUNT]
    ]

    # Create the needstate level sum of Optim Facings and Opt Sales.
    pdf_NS_item_facing_sum = (
        pdf_item_facing[main_level_keys + [n.F_OPT_FACINGS, n.F_ITEM_COUNT]]
        .groupby(main_level_keys)
        .sum()
        .reset_index()
        .rename(
            columns={
                n.F_OPT_FACINGS: f"Sum_{n.F_OPT_FACINGS}",
                n.F_ITEM_COUNT: f"Sum_{n.F_ITEM_COUNT}",
            }
        )
    )

    # Merge the Sum Table and the Item Master Table to compute the proportion.
    pdf_item_facing = pdf_item_facing.merge(
        pdf_NS_item_facing_sum, on=main_level_keys, how="inner"
    )

    # Attach the Unit Proportion and Opt_Facing_Prop.
    pdf_item_facing = pdf_item_facing.assign(
        Optim_Facing_Prop_Perc=pdf_item_facing[n.F_OPT_FACINGS]
        / pdf_item_facing[f"Sum_{n.F_OPT_FACINGS}"]
    ).assign(
        Optim_Item_Prop_Perc=pdf_item_facing[n.F_ITEM_COUNT]
        / pdf_item_facing[f"Sum_{n.F_ITEM_COUNT}"]
    )

    # Remove The NA variables: If the item do not have any optimizations, which means both Optim_Facings and Sum_Optim_Facings are 0.
    # Then fill the Optim_Facing_Prop as 1. Same for Item Proportion.
    pdf_item_facing[
        ["Optim_Facing_Prop_Perc", "Optim_Item_Prop_Perc"]
    ] = pdf_item_facing[["Optim_Facing_Prop_Perc", "Optim_Item_Prop_Perc"]].fillna(1)

    # Create the reported variable.
    pdf_item_facing["Is_Fitted"] = (
        np.abs(
            pdf_item_facing["Optim_Item_Prop_Perc"]
            - pdf_item_facing["Optim_Facing_Prop_Perc"]
        )
        <= tol_perc
    )

    # Create the Summary message
    na_perc = 1 - sum(pdf_item_facing["Is_Fitted"]) / pdf_item_facing.shape[0]
    msg = f"Items: {convert_perc(na_perc)} of the optimized facings are not fitted to Unit Proportion at NeedState level within {convert_perc(tol_perc)} deviation."

    pdf_item_facing_rounded = round_qa_df(pdf_item_facing)

    sense_check_result = SenseCheckResult(
        pdf=pdf_item_facing_rounded,
        message=msg,
        percentage=na_perc,
        percentage_target=0,
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}",
    )

    return sense_check_result


def sense_check_pog_sold_items(
    df_pog_data, df_trx_data, test_id, sheet_name, tol_perc=0.2
):
    """

    Parameters
    ----------
    tol_perc: float
        The tolerance percentage for reporting message, only the stores with more than tol_perc deviated pog/txn items
        will be reported
    test_id: string
        The test id of this QA task
    sheet_name: string
        The excel sheet name for this QA task
    df_pog_data: spark.DataFrame
        The combined and validated POG data.
    df_trx_data: spark.DataFrame
        The raw transaction data.

    Returns
    -------
    sense_check_result: SenseCheckResult
        The final result for sense checks.

    """
    df_trx_data_filter = (
        df_trx_data.select("STORE_NO", "ITEM_NO", "STORE_PHYSICAL_LOCATION_NO")
        .withColumn("Is_Sold", F.lit("True"))
        .na.drop(how="any", subset=["STORE_NO", "ITEM_NO"])
        .drop_duplicates()
    )

    df_pog_data_filter = (
        df_pog_data.select("STORE", "ITEM_NO")
        .withColumnRenamed("STORE", "STORE_NO")
        .withColumn("Is_POGed", F.lit("True"))
        .na.drop(how="any", subset=["STORE_NO", "ITEM_NO"])
        .drop_duplicates()
    )

    df_pog_sold_items_master = df_trx_data_filter.join(
        df_pog_data_filter, on=["STORE_NO", "ITEM_NO"], how="outer"
    ).fillna("False", subset=["Is_Sold", "Is_POGed"])

    df_pog_sold_items_summary = df_pog_sold_items_master.groupBy(
        "STORE_NO", "STORE_PHYSICAL_LOCATION_NO", "Is_Sold", "Is_POGed"
    ).agg(F.count("ITEM_NO").alias("Count"))

    df_pog_sold_items_total = df_pog_sold_items_summary.groupBy(
        "STORE_NO", "STORE_PHYSICAL_LOCATION_NO"
    ).agg(F.sum("Count").alias("Total"))

    df_pog_sold_items_summary = df_pog_sold_items_summary.join(
        df_pog_sold_items_total,
        on=["STORE_NO", "STORE_PHYSICAL_LOCATION_NO"],
        how="inner",
    )

    df_pog_sold_items_summary = df_pog_sold_items_summary.withColumn(
        "Count_Perc", df_pog_sold_items_summary.Count / df_pog_sold_items_summary.Total
    )

    # Transfer to dataframe and filter by Count Perc
    total_stores = df_pog_sold_items_summary.count()
    df_pog_sold_items_summary = df_pog_sold_items_summary.filter(
        f"Count_Perc >= {tol_perc}"
    ).toPandas()

    na_perc = df_pog_sold_items_summary.shape[0] / total_stores

    summary_view_msg = f"Stores: {convert_perc(na_perc)} percent of the stores have more than {convert_perc(tol_perc)} items either POGed but not sold, or vice versa."

    sense_check_result = SenseCheckResult(
        pdf=round_qa_df(df_pog_sold_items_summary),
        message=summary_view_msg,
        percentage=na_perc,
        percentage_target=tol_perc,
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}",
    )

    return sense_check_result


###############################################################################################################
# Summary Views
# Report the key metrics and data to csv files
###############################################################################################################


def summary_view_optimized_space_fits(
    pdf_legal_breaks,
    pdf_item_master,
    fits_perc: float,
    test_id: str,
    sheet_name: str,
    operation="perc",
) -> Tuple[SenseCheckResult, SenseCheckResult]:
    """
    This function take the item_master as input and check whether the item fits for categories
    in each store, and summarize the result into a pivot table. The y dimensions are Region_Desc
    and Banner level and x dimention is category.

    Parameters
    ----------
    pdf_legal_breaks: pd.DataFrame
        legal breaks dataframe with breaks information at store level, retrieved from the optimization result
    pdf_item_master: pd.DataFrame
        item master dataframe with item information, retrieved from the optimization result
    fits_perc: float
        the percentage of extra space allowed to be considering as fit
        If fits_perc = 0.02, then for items opt_lin_space/theo_lin_space <= 1.02 will be considering fits.
    operation: string
        the unit of pivot entry, must be one of ["perc", "sum"]
    test_id: str
        a string representing an ID (order number) in the summary view
    sheet_name: str
        sheet name in excel that would contain dataset containing the results

    Returns
    -------
    result_pivot: SenseCheckResult
        The result pivot table, which reports the percentage of the stores that fits for each Region Banner Cluster
    result_details: SenseCheckResult
        The result detailed dataframe, which contains all the information of the store that does not fits
    """
    n = get_col_names()
    # Create the Local Variables that will be used across functions
    operations = {
        "perc": lambda x: round(np.nansum(x) / np.shape(x)[0] * 100, 2),
        "sum": lambda x: np.nansum(x),
    }

    units = {"perc": "Perc of Store fits", "sum": "Count of Store fits"}

    does_fit = f"Does_Fit_In_{fits_perc}_Perc"
    dim = [
        n.F_SECTION_MASTER,
        n.F_STORE_PHYS_NO,
        n.F_REGION_DESC,
        n.F_BANNER,
        n.F_DEPARTMENT,
    ]

    # Input check
    if operation not in operations.keys():
        print(
            f"Operation {operation} is not supported, please choose one of {list(operations.keys())}. Change to percentage by default."
        )
        operation = "perc"

    # Select the columns that are essential for use
    pdf_item_master_section_level = (
        pdf_item_master[dim + [n.F_CUR_WIDTH_X_FAC, n.F_OPT_WIDTH_X_FAC]]
        .groupby(dim)
        .sum()
    )

    pdf_space_master = pdf_legal_breaks.merge(
        pdf_item_master_section_level, on=dim, how="inner"
    )

    # Check how much width the opt width goes over, and whether it fits
    # The output table would be df_space_fits_master contains the store level fits data
    pdf_space_master["Opt_Minus_Theo_Lin_Space"] = (
        pdf_space_master[n.F_OPT_WIDTH_X_FAC]
        - pdf_space_master["Cur_Theoretic_Lin_Space"]
    )
    pdf_space_master["Opt_Minus_Theo_Lin_Space_Perc"] = (
        pdf_space_master["Opt_Minus_Theo_Lin_Space"]
        / pdf_space_master["Cur_Theoretic_Lin_Space"]
    )
    pdf_space_master[does_fit] = (
        pdf_space_master["Opt_Minus_Theo_Lin_Space_Perc"] <= fits_perc
    )
    pdf_space_fits_master = pdf_space_master[
        dim + ["Opt_Minus_Theo_Lin_Space", "Opt_Minus_Theo_Lin_Space_Perc", does_fit]
    ]

    # Create the pivot table with index to be ['Region_Desc', 'Banner', 'Department'] and column to be [
    # 'Section_Master']
    pivot_table = pd.pivot_table(
        pdf_space_fits_master,
        values=does_fit,
        index=[n.F_REGION_DESC, n.F_BANNER],
        columns=[n.F_SECTION_MASTER],
        aggfunc=operations[operation],
    ).reset_index()

    # Create the detailed table to locate the unfitted stores
    pdf_detailed_table = pdf_space_fits_master.sort_values(does_fit, ascending=True)

    # Return a report message like units, operation and fit_in_perc
    summary_view_msg = (
        f"summary_view_optimized_space_fits: (Unit: {units[operation]} - Operation: {operation} - "
        f"Fit_In_Perc: {fits_perc}) "
    )

    result_pivot = SenseCheckResult(
        pdf=pivot_table,
        message=summary_view_msg,
        percentage="Not Implement Yet",
        percentage_target="Not Implement Yet",
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}-summary",
    )

    result_details = SenseCheckResult(
        pdf=round_qa_df(pdf_detailed_table),
        message=summary_view_msg,
        percentage="Not Implement Yet",
        percentage_target="Not Implement Yet",
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}-detail",
    )

    return result_pivot, result_details


def summary_view_top_x_items_per_ns(
    pdf_item_master,
    dependent_var,
    test_id: str,
    sheet_name: str,
    within="Cluster",
    top_n=5,
) -> SenseCheckResult:
    """This function take the item_master as input and create a table that display the information of the top n
    items sorted by dependent_var for each need state

    Parameters
    ----------
    pdf_item_master: pd.DataFrame
        Item master dataframe with item information, retrieved from the optimization result
    dependent_var: string
        The interested col that will be ordered by, has to be one of Opt_Margin, Cur_Margin, Opt_Sales, Cur_Sales or self_created_columns
    test_id: str
        a string representing an ID (order number) in the summary view
    sheet_name: str
        sheet name in excel that would contain dataset containing the results
    within: string
        The level want to get the top X items with in NeedState, must be one of ["RegionBanner", "Cluster", "Store"]
    top_n: int
        The top n items per need state

    Returns
    -------
    sense_check_result: SenseCheckResult
        The result of the top X items for each NeedState, by default in the Cluster level.
    """
    n = get_col_names()
    # Input Check for the dependent_var
    if dependent_var not in pdf_item_master.columns:
        print(
            f"dependent_var:{dependent_var} has to be one of Opt_Margin, Cur_Margin, Opt_Sales, Cur_Sales or self_created_columns"
        )
        return

    # You could calculate the Top N items at the following levels.
    main_level = {
        "RegionBanner": [
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_DEPARTMENT,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
        ],
        "Cluster": [
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_M_CLUSTER,
            n.F_DEPARTMENT,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
        ],
        "Store": [
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_M_CLUSTER,
            n.F_STORE_PHYS_NO,
            n.F_DEPARTMENT,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
        ],
    }

    # Copy the data to avoid any change on the original dataframe
    pdf_item_master = pdf_item_master.copy()

    # Create the dataframe to report
    pdf_group_by_NS = (
        pdf_item_master[
            main_level[within] + [n.F_ITEM_NO, n.F_ITEM_NAME, dependent_var]
        ]
        .groupby(main_level[within] + [n.F_ITEM_NO, n.F_ITEM_NAME])
        .sum()
        .reset_index()
        .sort_values(main_level[within] + [dependent_var], ascending=False)
        .groupby(main_level[within])
        .head(top_n)
    )

    pdf_group_by_NS.rename(
        columns={dependent_var: f"{dependent_var}_Sum"}, inplace=True
    )

    msg = f"summary_view_top_X_items_per_NS: Select {top_n} NeedState by {dependent_var} at {within} level."

    sense_check_result = SenseCheckResult(
        pdf=round_qa_df(pdf_group_by_NS),
        message=msg,
        percentage="NoNeed",
        percentage_target="NoNeed",
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}",
    )

    return sense_check_result


def summary_view_top_need_state_space_change(
    pdf_item_master,
    interested_col,
    test_id: str,
    sheet_name: str,
    operation="sum",
    top_n=5,
) -> Tuple[SenseCheckResult, SenseCheckResult]:
    """
    This function take the item_master as input and create a pivot table with needstate and section
    master as indexes, and cluster as columns. Then generate all the changes for interested col for the first table,
    and the top & bottom n needstates change within each cluster.

    Parameters
    ----------
    pdf_item_master: pd.DataFrame
        item master dataframe with item information, retrieved from the optimization result.
    interested_col: string
        the interested col that will be aggregate using operation.
    test_id: str
        a string representing an ID (order number) in the summary view
    sheet_name: str
        sheet name in excel that would contain dataset containing the results
    operation: string
        the operation used to aggregate on interested col, one of ["sum", "avg", "sum_abs", "avg_abs"].
    top_n: int
        the top&bottom n NeedStates within each cluster to be output at summary_table.

    Returns
    -------
    result_detail: SenseCheckResult
        The detailed pivot table that shows the space change for each NeedState
    result_summary:
        The bottom N NeedState and the top N NeedState based on space change for each Region Banner Cluster.
    """
    n = get_col_names()
    # Create the Local Variables that will be used across functions
    operations = {
        "sum": lambda x: np.nansum(x),
        "avg": lambda x: np.mean(x),
        "sum_abs": lambda x: np.nansum(np.abs(x)),
        "avg_abs": lambda x: np.avg(np.abs(x)),
    }

    main_level_keys = [
        n.F_REGION_DESC,
        n.F_BANNER,
        n.F_M_CLUSTER,
        n.F_SECTION_MASTER,
        n.F_NEED_STATE,
    ]

    # Create a copy of the dataset to void change on the original dataset
    pdf_item_master = pdf_item_master.copy().dropna(subset=main_level_keys)

    # Attach the top items to df_item_master
    qa_result_top_x_items_per_ns_cur = summary_view_top_x_items_per_ns(
        pdf_item_master=pdf_item_master,
        dependent_var=n.F_CUR_SALES,
        test_id="dummy",
        within="Cluster",
        top_n=1,
        sheet_name="dummy",
    )

    top_items = qa_result_top_x_items_per_ns_cur.pdf

    top_items = top_items.rename(columns={n.F_ITEM_NAME: "Top_Item"})
    pdf_item_master = pdf_item_master.merge(
        top_items[main_level_keys + ["Top_Item"]],
        on=main_level_keys,
        how="left",
    )

    # Create the pivot table and add Col_Sum and Row_Sum
    table = pd.pivot_table(
        pdf_item_master,
        values=interested_col,
        index=[
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
            "Top_Item",
        ],
        columns=[n.F_M_CLUSTER],
        aggfunc=operations[operation],
    ).fillna(0)

    # Create the top n and bottom n facing change
    n_summary_table = table.reset_index().melt(
        id_vars=[
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
            "Top_Item",
        ],
        value_name=f"{interested_col}_{operation}",
    )

    top_n_ns = (
        n_summary_table.sort_values(
            [
                n.F_M_CLUSTER,
                f"{interested_col}_{operation}",
                n.F_SECTION_MASTER,
                n.F_NEED_STATE,
            ],
            ascending=False,
        )
        .groupby(["M_Cluster"])
        .head(top_n)
        .assign(Top_NS=True)
    )

    bot_n_ns = (
        n_summary_table.sort_values(
            [
                n.F_M_CLUSTER,
                f"{interested_col}_{operation}",
                n.F_SECTION_MASTER,
                n.F_NEED_STATE,
            ],
            ascending=True,
        )
        .groupby([n.F_M_CLUSTER])
        .head(top_n)
        .assign(Top_NS=False)
    )

    summary_table = pd.concat([top_n_ns, bot_n_ns], axis=0)

    # Create the summary to store important information and attach to column name
    summary_view_msg = f"summary_view_top_need_state_space_change: Interested_Col: {interested_col} - Operation: {operation}"

    result_detail = SenseCheckResult(
        pdf=table.reset_index(),
        message=summary_view_msg,
        percentage="NoNeed",
        percentage_target="NoNeed",
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}-pivot",
    )

    result_summary = SenseCheckResult(
        pdf=summary_table,
        message=summary_view_msg,
        percentage="NoNeed",
        percentage_target="NoNeed",
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}-details",
    )

    return result_detail, result_summary


def summary_view_top_ns_pen_dep_by_cluster(
    df_bay_data,
    df_item_master,
    test_id: str,
    sheet_name: str,
    top_n=5,
    dependent_var="Sales",
) -> SenseCheckResult:
    """
    This function take the item_master as input and create the summary table contains the top n needstate within
    each cluster by index corrected sales penetration for that department and the top items for that NS (to get a sense
    for that NS)

    Parameters
    ----------
    n: spaceprod.utils.names.ColumnNames
        The column names that will used
    df_bay_data: pyspark.sql.dataframe.DataFrame
        this is the dataframe that has the all the real transaction data.
    df_item_master: pyspark.sql.dataframe.DataFrame
        item master dataframe with item information, retrieved from the optimization result
    top_n: int
        the integer the indicate how many top needstates we want to get
    dependent_var: string
        the name of the dependent_var, either Sales or Margin
    test_id: str
        a string representing an ID (order number) in the summary view
    sheet_name: str
        sheet name in excel that would contain dataset containing the results

    Returns
    -------
    sense_check_result: SenseCheckResult
        The result of the top N NeedState by sales penetration within each cluster
    """
    n = get_col_names()
    # Calculate the sales penetration and the top items list
    pdf_pen_data = cal_penetration_from_actual(df_bay_data, df_item_master)
    pdf_item_master = infer_dtypes(df_item_master.toPandas())
    pdf_item_master = pdf_item_master.rename(
        columns={"REGION": "Region_Desc"}
    )  # Unstable code, should not hard code, remove in prod

    qa_result_top_x_items_per_ns = summary_view_top_x_items_per_ns(
        pdf_item_master=pdf_item_master,
        dependent_var=n.F_CUR_SALES,
        test_id="dummy",
        within="Cluster",
        top_n=1,
        sheet_name="dummy",
    )

    top_items = qa_result_top_x_items_per_ns.pdf

    # Get the top n NS with in each cluster rank by sales penetration index
    pdf_pen_data["Facing_Change"] = (
        pdf_pen_data["Cluster_NS_Sum_Opt_Facing"]
        - pdf_pen_data["Cluster_NS_Sum_Cur_Facing"]
    )
    pdf_pen_top_n = (
        pdf_pen_data[
            [
                n.F_REGION_DESC,
                n.F_BANNER,
                n.F_M_CLUSTER,
                n.F_SECTION_MASTER,
                n.F_NEED_STATE,
                f"{dependent_var}_Pen_Index",
                "Cluster_NS_Sum_Opt_Facing",
                "Cluster_NS_Sum_Cur_Facing",
                "Facing_Change",
            ]
        ]
        .sort_values(
            [n.F_REGION_DESC, n.F_BANNER, n.F_M_CLUSTER, f"{dependent_var}_Pen_Index"],
            ascending=False,
        )
        .groupby([n.F_REGION_DESC, n.F_BANNER, n.F_M_CLUSTER])
        .head(top_n)
        .rename(
            columns={
                "Cluster_NS_Sum_Opt_Facing": "Sum_Opt_Facing",
                "Cluster_NS_Sum_Cur_Facing": "Sum_Cur_Facing",
            }
        )
    )

    # Combine with the top items column for better understands
    summary_table = pdf_pen_top_n.merge(
        top_items[
            [
                n.F_REGION_DESC,
                n.F_BANNER,
                n.F_M_CLUSTER,
                n.F_SECTION_MASTER,
                n.F_NEED_STATE,
                n.F_ITEM_NAME,
            ]
        ],
        on=[
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_M_CLUSTER,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
        ],
        how="left",
    ).rename(columns={"Item_Name": "Top_Item"})

    msg = f"summary_view_top_NS_pen_Dep_By_Cluster: Dependent Variable - {dependent_var} Top N Items - {top_n}"

    result = SenseCheckResult(
        pdf=summary_table,
        message=msg,
        percentage="NoNeed",
        percentage_target="NoNeed",
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}",
    )

    return result


def summary_view_delisted_items(
    df_item_master,
    df_trx_data,
    test_id,
    sheet_name,
    unique_customers_tol,
    cross_shop_tol_perc,
):
    """

    Parameters
    ----------
    df_item_master: spark.DataFrame
        The spark dataframe of output master.
    df_trx_data: spark.DataFrame
        The spark dataframe of transaction data.
    test_id: string
        The test id for this QA check.
    sheet_name: string
        The name of the excel sheet.
    unique_customers_tol: float
        The unique customers tolerance. Only items purchased by MORE than [unique_customers_tol_perc] will be reported.
    delisted_items_tol_perc: float
        The percentage of cross shop customers tolerance. Only items purchased by LESS than [cross_shop_tol_perc] will be reported.

    Returns
    -------
    sense_check_result: SenseCheckResult
        The result to check the delisted items, which contains the report dataframe and report message
    """
    n = get_col_names()

    # Select the useful columns and the delisted flag columns
    df_item_master_filtered = (
        df_item_master.select(
            n.F_STORE_PHYS_NO,
            n.F_ITEM_NO,
            n.F_ITEM_NAME,
            n.F_CUR_FACINGS,
            n.F_OPT_FACINGS,
            n.F_CUR_SALES,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
        )
        .filter(f"{n.F_CUR_FACINGS} > 0")
        .withColumn(
            "Is_delisted",
            F.when(
                (F.col(n.F_OPT_FACINGS) == 0) & (F.col(n.F_CUR_FACINGS) > 0), 1
            ).otherwise(0),
        )
    )

    # Select the useful columns from the transaction data
    # -1 & 1 Customer_Card_ID are non card transactions (loyalty card etc)
    df_trx_data_filtered = (
        df_trx_data.select("CUSTOMER_CARD_SK", "ITEM_NO", "STORE_PHYSICAL_LOCATION_NO")
        .withColumnRenamed("ITEM_NO", n.F_ITEM_NO)
        .withColumnRenamed("CUSTOMER_CARD_SK", "Customer_Card_ID")
        .withColumnRenamed("STORE_PHYSICAL_LOCATION_NO", n.F_STORE_PHYS_NO)
        .filter("Customer_Card_ID > 1")
        .drop_duplicates()
    )

    # Match the transaction data with the item master data
    df_customer_item_master = df_item_master_filtered.join(
        df_trx_data_filtered, on=[n.F_STORE_PHYS_NO, n.F_ITEM_NO], how="inner"
    )

    # Split the listed and unlisted data
    df_customer_item_delisted = df_customer_item_master.filter(
        f"{n.F_OPT_FACINGS} == 0"
    ).select(
        n.F_STORE_PHYS_NO,
        n.F_ITEM_NO,
        n.F_SECTION_MASTER,
        n.F_NEED_STATE,
        "Customer_Card_ID",
    )

    df_customer_item_listed = (
        df_customer_item_master.filter(f"{n.F_OPT_FACINGS} != 0")
        .select(
            n.F_STORE_PHYS_NO,
            n.F_ITEM_NO,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
            "Customer_Card_ID",
        )
        .withColumnRenamed(n.F_ITEM_NO, "Item_No_L")
    )

    # Calculate the Cross Shop Customers
    # The definition of Cross shop customer is the customer who bought delisted item before, and bought a list item after
    df_customer_item_match = (
        df_customer_item_delisted.join(
            df_customer_item_listed,
            on=[
                n.F_STORE_PHYS_NO,
                n.F_SECTION_MASTER,
                n.F_NEED_STATE,
                "Customer_Card_ID",
            ],
            how="inner",
        )
        .filter(f"{n.F_ITEM_NO} != Item_No_L")
        .select(
            n.F_STORE_PHYS_NO,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
            "Customer_Card_ID",
            n.F_ITEM_NO,
        )
        .drop_duplicates()
        .groupby(n.F_STORE_PHYS_NO, n.F_SECTION_MASTER, n.F_NEED_STATE, n.F_ITEM_NO)
        .agg(F.count("Customer_Card_ID").alias("Cross_Shop_Customers_Count"))
    )

    df_item_delisted = df_customer_item_delisted.groupby(
        n.F_STORE_PHYS_NO, n.F_ITEM_NO, n.F_SECTION_MASTER, n.F_NEED_STATE
    ).agg(F.count("Customer_Card_ID").alias("Customers_Count"))

    df_item_delisted_report = (
        df_customer_item_match.join(
            df_item_delisted,
            on=[n.F_STORE_PHYS_NO, n.F_ITEM_NO, n.F_SECTION_MASTER, n.F_NEED_STATE],
            how="right",
        )
        .fillna(0, subset=["Cross_Shop_Customers_Count"])
        .withColumn(
            "Cross_Shop_Customers_Perc",
            F.col("Cross_Shop_Customers_Count") / F.col("Customers_Count"),
        )
    )

    df_item_delisted_final = (
        df_item_delisted_report.join(
            df_item_master_filtered,
            on=[n.F_STORE_PHYS_NO, n.F_ITEM_NO, n.F_SECTION_MASTER, n.F_NEED_STATE],
            how="inner",
        )
        .filter(
            f"Customers_Count >= {unique_customers_tol} and Cross_Shop_Customers_Perc <= {cross_shop_tol_perc}"
        )
        .toPandas()
    )

    msg = f"summary_view_delisted_items: The filter threshold for Number of Unique Customers {unique_customers_tol}, for Cross_Shop_Customers_Perc {convert_perc(cross_shop_tol_perc)}"

    sense_check_result = SenseCheckResult(
        pdf=round_qa_df(df_item_delisted_final),
        message=msg,
        percentage="NoNeed",
        percentage_target="NoNeed",
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}",
    )

    return sense_check_result


def summary_view_detect_ns_outliers(
    pdf_pen_data,
    z_score,
    context_folder_path,
    test_id: str,
    sheet_name: str,
    test_flag=False,
):
    """This is the plot function used to assess the goodness of fit with in each cluster by justifying the fit
    on the NeedState level. The outlier detection algorithm used 95 confidence interval on intercept only. (The 95%
    confidence interval is consistent to slope's standard deviation). The plot compares optimized facings penetration (
    X axis) and real sales penetration (Y axis). The plots will be saved at sense_check/NS_outlier_plots

    Parameters
    ----------
    pdf_pen_data: pd.DataFrame
        the penetration dataframe master
    z_score: float:
        The z_score for outliers detection, ie: 95% - Z Score 1.96
    context_folder_path: string
        the folder path to current Run
    test_id: str
        a string representing an ID (order number) in the summary view
    sheet_name: str
        sheet name in excel that would contain dataset containing the results
    test_flag: bool
        if this function being tested? True if it is being tested. False if it is not.

    Returns
    -------
    result_outliers: SenseCheckResult
        The SenseCheckResult object contains the data of the outliers used to draw the plots
    result_master: SenseCheckResult
        The SenseCheckResult object contains the master data of the outliers, which contains the detailed information
        for each NeedState and a flag column to identify whether each NeedState is an outlier.
    """
    ns_outliers, ns_data_master, plot_info = detect_outliers(pdf_pen_data, z_score)

    plot_cluster_scatter_facing_pen_vs_sales_pen(
        ns_outliers=ns_outliers,
        ns_data_master=ns_data_master,
        plot_info_master=plot_info,
        z_score=z_score,
        context_folder_path=context_folder_path,
        test_flag=test_flag,
    )

    # Construct the report message

    msg = f"NeedState: There are {ns_outliers.shape[0]} NeedState outliers by 97.5% confidence interval. Plots saved at sense_check/NS_outlier_plots."

    result_outliers = SenseCheckResult(
        pdf=ns_outliers,
        message=msg,
        percentage="NoNeed",
        percentage_target="NoNeed",
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}",
    )

    result_master = SenseCheckResult(
        pdf=ns_data_master,
        message=msg,
        percentage="NoNeed",
        percentage_target="NoNeed",
        test_id=test_id,
        sheet_name=f"{test_id}-{sheet_name}-master",
    )

    return result_outliers, result_master


def generate_the_list_of_qa_results(
    qa_result_uplift_by_section_sales_dollars: SenseCheckResult,
    qa_result_uplift_by_section_sales_facings: SenseCheckResult,
    qa_result_uplift_by_section_margin_dollars: SenseCheckResult,
    qa_result_uplift_by_section_margin_facings: SenseCheckResult,
    qa_result_ns_assignment: SenseCheckResult,
    qa_result_cluster_assignemnt: SenseCheckResult,
    qa_result_ns_facing: SenseCheckResult,
    qa_result_opt_cat_max_break_changes: SenseCheckResult,
    qa_result_opt_breaks_int: SenseCheckResult,
    qa_result_opt_space_fits_summary: SenseCheckResult,
    qa_result_opt_space_fits_detail: SenseCheckResult,
    qa_result_item_max_opt_facings: SenseCheckResult,
    qa_result_top_x_items_per_ns_cur: SenseCheckResult,
    qa_result_top_x_items_per_ns_opt: SenseCheckResult,
    qa_result_top_ns_space_change_detail: SenseCheckResult,
    qa_result_top_ns_space_change_summary: SenseCheckResult,
    qa_result_assign_facing_by_unitprop: SenseCheckResult,
    qa_result_top_pen_ns_by_pen_index_sales: SenseCheckResult,
    qa_result_top_pen_ns_by_pen_index_margin: SenseCheckResult,
    qa_result_ns_outliers_by_sales_master: SenseCheckResult,
    qa_result_delisted_items: SenseCheckResult,
    qa_result_pog_sold_items: SenseCheckResult,
):
    """
    Collects all the requires QA sense check result objects
    into a single list in order to prepare for the Excel population

    It also pre-appends a "summary" QA sense check result.
    """

    # first put the required sense check objects in to a list
    # NOTE: the order of this list matters, this is the order
    # in which the sense check results will be listed on the summary view
    # as well as the how the Excel sheets will be ordered
    list_qa_results = [
        qa_result_uplift_by_section_sales_dollars,
        qa_result_uplift_by_section_sales_facings,
        qa_result_uplift_by_section_margin_dollars,
        qa_result_uplift_by_section_margin_facings,
        qa_result_ns_assignment,
        qa_result_cluster_assignemnt,
        qa_result_ns_facing,
        qa_result_opt_cat_max_break_changes,
        qa_result_opt_breaks_int,
        qa_result_opt_space_fits_summary,
        qa_result_opt_space_fits_detail,
        qa_result_item_max_opt_facings,
        qa_result_top_x_items_per_ns_cur,
        qa_result_top_x_items_per_ns_opt,
        qa_result_top_ns_space_change_detail,
        qa_result_top_ns_space_change_summary,
        qa_result_assign_facing_by_unitprop,
        qa_result_top_pen_ns_by_pen_index_sales,
        qa_result_top_pen_ns_by_pen_index_margin,
        qa_result_ns_outliers_by_sales_master,
        qa_result_delisted_items,
        qa_result_pog_sold_items,
    ]

    # check to ensure we did not include the same sense check result twice
    msg = "You have duplicates in your 'list_qa_results'. Please fix."
    assert len(set(list_qa_results)) == len(list_qa_results), msg

    # create a "summary" view to be included as the first tab in Excel
    data = [x.get_summary_record() for x in list_qa_results]
    cols = ["test_id", "QA_Status", "message"]
    df_summary = spark.createDataFrame(data=data, schema=cols)
    pdf_summary = df_summary.toPandas()

    # pre-append the summary dataframe as a "Sense check result"
    # so that we can process it together with the rest of the sense check
    # results and include it as the 1st sheet in the final Excel report
    qa_result_summary = SenseCheckResult(
        pdf=pdf_summary,
        message=None,
        percentage=None,
        percentage_target=None,
        test_id=None,
        sheet_name="QA_check_summary",
    )

    list_qa_results = [qa_result_summary] + list_qa_results

    # ensure we don't have duplicate sheet names
    # otherwise the excel sheets will overwrite each other, and we will lose
    # data
    list_sheet_names = [x.sheet_name for x in list_qa_results]
    dups = list(set([x for x in list_sheet_names if list_sheet_names.count(x) > 1]))
    msg = f"The following duplicate sheet names found, please fix: {dups}"
    assert len(dups) == 0, msg

    return list_qa_results


def cal_penetration_from_actual(
    df_bay_data, df_additional_info, include_opt_facing_pen=True
):
    """This function calculate the actual sales/margin penetration by NeedState in Cluster, Overall as well as the index
    (All sales need to be in department level)

    Sale_Pen_Cluster_NS = Sales_in_Cluster_A_NS_B / Sales_in_Cluster_A
    Sales_Pen_NS = Sales_in_NS_B / Sales

    Parameters
    ----------
    df_bay_data: pyspark.sql.dataframe.DataFrame
        this is the dataframe that has the all the real transaction data.
    df_additional_info: pyspark.sql.dataframe.DataFrame
        if include_opt_facing_pen == True, then df_additional_info = df_item_master to include the department level data and optimum facing penetration.
        - The objective is to use the good neestates sales penetration and optimum facings penetration to get the fit curve.
        if include_opt_facing_pen == False, then df_addtional_info = df_section_department to include the department level data only.
        - The objective is to use the bad needstates sales penetration to project the optimum facings penetration.
        Check the reference for more information.
    include_opt_facing_pen: bool
        should the final dataframe include the opt_facing_penetration

    Returns
    -------
    df_actual_penetration: pd.DataFrame
        this dataframe contains all the information of penetration from the real data.
    """
    n = get_col_names()
    df_bay_data_selected = (
        df_bay_data.withColumnRenamed("ITEM_NO", n.F_ITEM_NO)
        .withColumnRenamed("ITEM_NAME", n.F_ITEM_NAME)
        .withColumnRenamed("NATIONAL_BANNER_DESC", n.F_BANNER)
        .withColumnRenamed("REGION", n.F_REGION_DESC)
        .withColumnRenamed("STORE_PHYSICAL_LOCATION_NO", n.F_STORE_PHYS_NO)
        .withColumnRenamed("need_state", n.F_NEED_STATE)
        .withColumnRenamed("E2E_MARGIN_TOTAL", n.F_MARGIN)
        .withColumnRenamed("SECTION_MASTER", n.F_SECTION_MASTER)
        .withColumnRenamed("MERGED_CLUSTER", n.F_M_CLUSTER)
        .select(
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_M_CLUSTER,
            n.F_STORE_PHYS_NO,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
            n.F_ITEM_NO,
            n.F_ITEM_NAME,
            "Margin",
            "Sales",
        )
        .fillna(0, subset=["Margin", "Sales"])
    )

    if include_opt_facing_pen:
        df_additional_info_selected = df_additional_info.select(
            n.F_STORE_PHYS_NO,
            n.F_DEPARTMENT,
            n.F_SECTION_MASTER,
            n.F_NEED_STATE,
            n.F_ITEM_NO,
            n.F_CUR_FACINGS,
            n.F_OPT_FACINGS,
        )

        df_bay_data_selected = df_bay_data_selected.join(
            df_additional_info_selected,
            on=[n.F_STORE_PHYS_NO, n.F_SECTION_MASTER, n.F_NEED_STATE, n.F_ITEM_NO],
            how="inner",
        ).fillna(0, subset=[n.F_OPT_FACINGS])

    else:
        df_bay_data_selected = (
            df_bay_data_selected.join(
                df_additional_info, on=[n.F_SECTION_MASTER], how="inner"
            )
            .withColumn(n.F_CUR_FACINGS, F.lit(None))
            .withColumn(n.F_OPT_FACINGS, F.lit(None))
        )

    df_bay_data_master = df_bay_data_selected.select(
        n.F_REGION_DESC,
        n.F_BANNER,
        n.F_M_CLUSTER,
        n.F_DEPARTMENT,
        n.F_SECTION_MASTER,
        n.F_NEED_STATE,
    ).drop_duplicates()

    # Create the Cluster Department level sum of sales/margin
    df_bay_data_selected_cluster_dep = (
        df_bay_data_selected.select(
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_M_CLUSTER,
            n.F_DEPARTMENT,
            n.F_STORE_PHYS_NO,
            "Margin",
            "Sales",
            n.F_OPT_FACINGS,
            n.F_CUR_FACINGS,
        )
        .groupBy(n.F_REGION_DESC, n.F_BANNER, n.F_M_CLUSTER, n.F_DEPARTMENT)
        .agg(
            F.sum("Margin").alias("Cluster_Dep_Sum_Margin"),
            F.sum("Sales").alias("Cluster_Dep_Sum_Sales"),
            F.sum(n.F_OPT_FACINGS).alias("Cluster_Dep_Sum_Opt_Facing"),
            F.sum(n.F_CUR_FACINGS).alias("Cluster_Dep_Sum_Cur_Facing"),
            F.countDistinct(n.F_STORE_PHYS_NO).alias("Cluster_Dep_Num_Stores"),
        )
    )

    # Create the Cluster NeedState level sum of sales/margin
    df_bay_data_selected_cluster_NS = (
        df_bay_data_selected.select(
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_M_CLUSTER,
            n.F_SECTION_MASTER,
            n.F_DEPARTMENT,
            n.F_STORE_PHYS_NO,
            n.F_NEED_STATE,
            "Margin",
            "Sales",
            n.F_OPT_FACINGS,
            n.F_CUR_FACINGS,
        )
        .groupBy(
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_M_CLUSTER,
            n.F_SECTION_MASTER,
            n.F_DEPARTMENT,
            n.F_NEED_STATE,
        )
        .agg(
            F.sum("Margin").alias("Cluster_NS_Sum_Margin"),
            F.sum("Sales").alias("Cluster_NS_Sum_Sales"),
            F.sum(n.F_OPT_FACINGS).alias("Cluster_NS_Sum_Opt_Facing"),
            F.sum(n.F_CUR_FACINGS).alias("Cluster_NS_Sum_Cur_Facing"),
            F.countDistinct(n.F_STORE_PHYS_NO).alias("Cluster_NS_Num_Stores"),
        )
    )

    # Create the RB Department level sum of sales/margin
    df_bay_data_selected_RB_dep = (
        df_bay_data_selected.select(
            n.F_REGION_DESC, n.F_BANNER, n.F_DEPARTMENT, "Margin", "Sales"
        )
        .groupBy(n.F_REGION_DESC, n.F_BANNER, n.F_DEPARTMENT)
        .agg(
            F.sum("Margin").alias("RB_Dep_Sum_Margin"),
            F.sum("Sales").alias("RB_Dep_Sum_Sales"),
        )
    )

    # Create the RB NeedState level sum of sales/margin
    df_bay_data_selected_RB_NS = (
        df_bay_data_selected.select(
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_SECTION_MASTER,
            n.F_DEPARTMENT,
            n.F_NEED_STATE,
            "Margin",
            "Sales",
        )
        .groupBy(
            n.F_REGION_DESC,
            n.F_BANNER,
            n.F_SECTION_MASTER,
            n.F_DEPARTMENT,
            n.F_NEED_STATE,
        )
        .agg(
            F.sum("Margin").alias("RB_NS_Sum_Margin"),
            F.sum("Sales").alias("RB_NS_Sum_Sales"),
        )
    )

    # Join all the data
    pdf_actual_penetration = (
        df_bay_data_master.join(
            df_bay_data_selected_cluster_NS,
            on=[
                n.F_REGION_DESC,
                n.F_BANNER,
                n.F_M_CLUSTER,
                n.F_SECTION_MASTER,
                n.F_DEPARTMENT,
                n.F_NEED_STATE,
            ],
            how="left",
        )
        .persist()
        .join(
            df_bay_data_selected_RB_NS,
            on=[
                n.F_REGION_DESC,
                n.F_BANNER,
                n.F_SECTION_MASTER,
                n.F_DEPARTMENT,
                n.F_NEED_STATE,
            ],
            how="left",
        )
        .persist()
        .join(
            df_bay_data_selected_cluster_dep,
            on=[n.F_REGION_DESC, n.F_BANNER, n.F_M_CLUSTER, n.F_DEPARTMENT],
            how="left",
        )
        .persist()
        .join(
            df_bay_data_selected_RB_dep,
            on=[n.F_REGION_DESC, n.F_BANNER, n.F_DEPARTMENT],
            how="left",
        )
        .toPandas()
    )

    pdf_actual_penetration_result = (
        pdf_actual_penetration.assign(
            Sales_Cluster_Pen=pdf_actual_penetration["Cluster_NS_Sum_Sales"]
            / pdf_actual_penetration["Cluster_Dep_Sum_Sales"]
        )
        .assign(
            Sales_RB_Pen=pdf_actual_penetration["RB_NS_Sum_Sales"]
            / pdf_actual_penetration["RB_Dep_Sum_Sales"]
        )
        .assign(
            Margin_Cluster_Pen=pdf_actual_penetration["Cluster_NS_Sum_Margin"]
            / pdf_actual_penetration["Cluster_Dep_Sum_Margin"]
        )
        .assign(
            Margin_RB_Pen=pdf_actual_penetration["RB_NS_Sum_Margin"]
            / pdf_actual_penetration["RB_Dep_Sum_Margin"]
        )
        .assign(
            Opt_Facing_Cluster_Pen=pdf_actual_penetration["Cluster_NS_Sum_Opt_Facing"]
            / pdf_actual_penetration["Cluster_Dep_Sum_Opt_Facing"]
        )
    )

    pdf_actual_penetration_result = pdf_actual_penetration_result.assign(
        Sales_Pen_Index=pdf_actual_penetration_result["Sales_Cluster_Pen"]
        / pdf_actual_penetration_result["Sales_RB_Pen"]
    ).assign(
        Margin_Pen_Index=pdf_actual_penetration_result["Margin_Cluster_Pen"]
        / pdf_actual_penetration_result["Margin_RB_Pen"]
    )

    return pdf_actual_penetration_result
