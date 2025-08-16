import numpy as np
import pandas as pd

from inno_utils.loggers import log
from spaceprod.src.system_integration.store_cluster_mapping import (
    get_door_numbers_by_store_dept,
)
from spaceprod.utils.data_transformation import (
    apply_data_definition,
    apply_data_definition_pd,
)
from spaceprod.utils.imports import F, SparkDataFrame, T
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.validation import dup_check, dup_check_pd


def opt_legal_float_filter(df_summary_legal_breaks: SparkDataFrame):
    """
     Filter float value of optimal_legal_breaks

     Parameters
     ----------
    df_summary_legal_breaks : SparkDataFrame
         information that caries legal breaks to calc # of doors (opt output)

     Returns
     -------
     summary_legal_breaks output without float optimal_legal_breaks value
    """
    pdf_result = df_summary_legal_breaks.toPandas()

    # get only non-integers
    pdf_result = pdf_result[pdf_result.Opt_Legal_Breaks.apply(float.is_integer)]

    return pdf_result


def add_department_size_information(
    df_task_opt_rerun_item_output_master_updated: SparkDataFrame,
    df_summary_legal_breaks_filtered: SparkDataFrame,
):
    """
    Adds department size information (number of doors) on store/dept level

    Parameters
    ----------
    df_task_opt_rerun_item_output_master_updated : SparkDataFrame
        final output of optimization

    df_summary_legal_breaks : SparkDataFrame
        information that caries legal breaks to calc # of doors (opt output)

    Returns
    -------
    optimization output with door information
    """

    # get the door lookup data
    pdf_doors = get_door_numbers_by_store_dept(df_summary_legal_breaks_filtered)
    dup_check_pd(
        pdf_doors, ["store_physical_location_id", "department", "section_master"]
    )

    # convert the main data to pandas and do a quick dup check
    pdf = df_task_opt_rerun_item_output_master_updated.toPandas()
    dims = ["Store_Physical_Location_No", "Section_Master", "Item_No"]
    dup_check_pd(pdf, dims)

    # apply data definition to our (converts col names/types)
    cols_mapping = [
        ("REGION", "region", str),
        ("Banner", "banner", str),
        ("M_Cluster", "m_cluster", str),
        ("Section_Master", "section_master", str),
        ("Item_No", "item_no", str),
        ("Store_Physical_Location_No", "store_physical_location_id", str),
        ("Need_State_Idx", "need_state_idx", int),
        ("Need_State", "need_state", int),
        ("Item_Name", "item_name", str),
        ("Lvl4_Name", "lvl4_name", str),
        ("Width", "width", float),
        ("Constant_Treatment", "constant_treatment", bool),
        ("Current_Facings", "current_facings", float),
        ("Cur_Sales", "cur_sales", float),
        ("Opt_Sales", "opt_sales", float),
        ("Optim_Facings", "optim_facings", float),
        ("Unique_Items_Cur_Assorted", "unique_items_cur_assorted", int),
        ("Unique_Items_Opt_Assorted", "unique_items_opt_assorted", int),
        ("Cur_Width_X_Facing", "cur_width_x_facing", float),
        ("Opt_Width_X_Facing", "opt_width_x_facing", float),
        ("Cur_Opt_Same_Facing", "cur_opt_same_facing", int),
        ("Opt_Minus_Cur_Facings", "opt_minus_cur_facings", float),
        ("Department", "department", str),
        ("Opt_Margin", "opt_margin", float),
        ("Cur_Margin", "cur_margin", float),
    ]

    pdf = apply_data_definition_pd(
        pdf=pdf,
        data_contract=cols_mapping,
    )

    pdf_result = pdf.merge(
        right=pdf_doors,
        on=["store_physical_location_id", "department", "section_master"],
        how="inner",
    )

    return pdf_result


def add_col_lin_space_per_break_lgl_inc_width(
    pdf: pd.DataFrame,
    df_summary_legal_breaks: SparkDataFrame,
):
    df_leg = df_summary_legal_breaks

    data_def = [
        ("Store_Physical_Location_No", "store_physical_location_id", T.StringType()),
        ("Section_Master", "section_master", T.StringType()),
        ("Department", "department", T.StringType()),
        ("Lin_Space_Per_Break", "lin_space_per_break", T.FloatType()),
        ("Legal_Increment_Width", "legal_increment_width", T.FloatType()),
    ]

    df_leg = apply_data_definition(
        df=df_leg,
        data_contract=data_def,
        drop_if_not_in_data_contract=True,
    )

    # the lin_space_per_break should consistent across each store/dep/sm
    # therefore if we dedup on all 3 columns, the data should still be unique
    # on store/dep/sm
    df_leg = df_leg.dropDuplicates()
    dims = ["store_physical_location_id", "department", "section_master"]
    dup_check(df_leg, dims)

    # now do the lookup for lin_space_per_break
    pdf_leg = df_leg.toPandas()
    dims = ["store_physical_location_id", "department", "section_master"]
    pdf = pdf.merge(pdf_leg, "left", dims)

    # check to ensure a clean join
    msg = "bad 'lin_space_per_break' lookup"
    assert not pdf["lin_space_per_break"].isnull().any(), msg

    # check to ensure a clean join
    msg = "bad 'legal_increment_width' lookup"
    assert not pdf["legal_increment_width"].isnull().any(), msg

    return pdf


def log_non_consistent_facings(
    df_opt_output_with_store_cluster_id: SparkDataFrame,
) -> None:
    """
    Logs the information on non-consistent facings per
    store_cluster_id / item_no combinations
    Parameters
    ----------
    df_opt_output_with_store_cluster_id : SparkDataFrame
        optimization final output with store_cluster_id column

    Returns
    -------
    None
    """

    pdf = df_opt_output_with_store_cluster_id.toPandas()

    # check how many  store_cluster_id / item_no combinations
    # have more than 1 value for optimal_facings
    dims = ["store_cluster_id", "item_no"]
    agg = {"optim_facings": pd.Series.nunique}
    pdf_agg = pdf.groupby(dims).agg(agg).reset_index()
    pdf_check = pdf_agg.query("optim_facings>1")

    if len(pdf_check) == 0:
        msg = """
        We did not find any records for which there is more than 1 value for 
        optimal_facings for a given store_cluster_id / item_no
        """

        log.info(msg)
        return

    n = len(pdf_check)
    n_ids = len(pdf_check["store_cluster_id"].unique())
    n_items = len(pdf_check["item_no"].unique())

    n_ttl = len(pdf_agg)
    n_ids_ttl = len(pdf_agg["store_cluster_id"].unique())
    n_items_ttl = len(pdf_agg["item_no"].unique())

    n_per = round((n / n_ttl) * 100, 1)
    n_ids_per = round((n_ids / n_ids_ttl) * 100, 1)
    n_items_per = round((n_items / n_items_ttl) * 100, 1)

    # TODO: to address
    msg = f"""
    We found records for which there is more than 1 value for 
    optimal_facings for a given store_cluster_id / item_no
    combination. There are {n} (out of {n_ttl}, {n_per}%) such combinations  
    across {n_ids} (out of {n_ids_ttl}, {n_ids_per}%) store_cluster_id's  
    and {n_items} (out of {n_items_ttl}, {n_items_per}%) items.
    We will be taking the MODE of optimal facings for these records.
    """

    log.info(msg)

    # EXAMPLE:
    # pdf.query("region=='ontario' and banner=='SOBEYS' and department=='Frozen' and item_no=='126284' and store_physical_location_id==['21','11489']")


def aggregate_opt_output_to_store_cluster_id_level(
    df_opt_output_with_store_cluster_id: SparkDataFrame,
) -> SparkDataFrame:
    """
    Aggregates optimal facings and additional required metrics
    to the store_cluster_id level,
    e.g.: ONT-SBY-FRZ-2B-65D
    normally there should be consistent optimal_facings value in this
    granularity level, but sometimes it is not (due to the logic in opt
    being not perfect), in those cases we are taking the MODE (most frequent
    value) between stores.
    In case if there are multiple modes (equally frequent values)
    we take the median and round it to the nearest integer.

    Parameters
    ----------
    df_opt_output_with_store_cluster_id : SparkDataFrame
        opt output with store_cluster_id column

    Returns
    -------
    aggregated opt output to the store_cluster_id / item_no level
    """

    pdf = df_opt_output_with_store_cluster_id.toPandas()

    dims = ["store_cluster_id", "item_no"]
    func_mode = lambda x: pd.Series.mode(x).tolist()

    agg = {
        "optim_facings": func_mode,
        "current_facings": func_mode,
        "lin_space_per_break": func_mode,
    }

    ren = {
        "optim_facings": "optim_facings_mode",
        "current_facings": "current_facings_mode",
        "lin_space_per_break": "lin_space_per_break_mode",
    }

    pdf_agg = pdf.groupby(dims).agg(agg).reset_index().rename(columns=ren)

    # there can be more than 1 mode for a given list of values when there
    # are values that are equally frequent. Therefore the result of this
    # aggregation should really be a list of numbers, not a single value.
    # we want to keep the optim_facings_mode column for future reference
    # converting its values to a consistent type (list of integers)
    # for compatibility with Spark
    # in case if there are multiple modes (equally frequent values)
    # we take the median and round it to the nearest integer
    func = lambda x: int(round(np.median(x), 0))
    pdf_agg["optim_facings"] = pdf_agg["optim_facings_mode"].apply(func)
    pdf_agg["current_facings"] = pdf_agg["current_facings_mode"].apply(func)
    pdf_agg["lin_space_per_break"] = pdf_agg["lin_space_per_break_mode"].apply(func)

    # add back additional attributes required downstream
    cols = [
        "store_cluster_id",
        "region",
        "banner",
        "m_cluster",
        "section_master",
        "item_no",
        "need_state",
        "department",
    ]

    # TODO: consider bringing more columns than above in order to also have
    #  those columns on store_cluster_id / item level

    # the attribute data should be on store_cluster_id / item_no level
    # even after we remove the store dimension. Do a dup check to ensure that
    # the dimensionality still holds
    pdf_attr = pdf[cols].drop_duplicates()
    dims = ["store_cluster_id", "item_no"]
    dup_check_pd(pdf_attr, dims)

    # join back to our original dataset
    pdf_agg = pdf_agg.merge(right=pdf_attr, on=dims, how="left")

    # ensure the join is clean
    msg = f"bad join of required attributes: {cols}"
    assert all([not pdf_agg[x].isnull().any() for x in cols]), msg

    df = spark.createDataFrame(pdf_agg)

    return df


def log_entity_counts_opt_output(
    df_opt_output_with_store_cluster_id: SparkDataFrame,
) -> None:
    """
    A simple function that summarizes the entity counts for the
    final optimization output. Useful to know for which scope the opt
    result was generated.
    Parameters
    ----------
    df_opt_output_with_store_cluster_id : SparkDataFrame
        final optimization output

    Returns
    -------
    None
    """

    # shortcut
    df = df_opt_output_with_store_cluster_id

    cols = [
        "department",
        "m_cluster",
        "section_master",
        "store_cluster_id",
        "store_physical_location_id",
        "item_no",
    ]

    agg = [
        F.concat(
            F.countDistinct(x),
            F.lit(": "),
            F.concat_ws(", ", F.slice(F.collect_set(x), start=1, length=5)),
            F.lit(" ..."),
        ).alias(x)
        for x in cols
    ]
    agg = [F.count("*").alias("rows")] + agg
    ord = [F.col("rows").desc()]

    pdf_overall = df.groupBy(F.lit("0")).agg(*agg).orderBy(*ord).toPandas()
    pdf_reg = df.groupBy("region").agg(*agg).orderBy(*ord).toPandas()
    pdf_reg_ban = df.groupBy("region", "banner").agg(*agg).orderBy(*ord).toPandas()

    msg = f"""
    Here is the entity count summary along with examples of values.
    
    Structure: 
    
    |-----------------------------------------------------------|
    | Entity Name                                               |
    |-----------------------------------------------------------|
    | N: Example Value 1, Example Value 2, Example Value 3, ... |
    |-----------------------------------------------------------|
    
    Where N is total number of distinct values

    OVERALL:
    \n{pdf_overall.T.to_markdown()}

    BY REGION:
    \n{pdf_reg.to_markdown(index=False)}

    BY REGION AND BANNER:
    \n{pdf_reg_ban.to_markdown(index=False)}
    """

    log.info(msg)


def add_col_width(
    df: SparkDataFrame,
    df_opt_output_with_store_cluster_id: SparkDataFrame,
):
    """
    Adds 'width' column to the final aggregated output of optimization
    """

    # there should only be 1 width value per item, thus this dup check
    # should hold
    dims = ["item_no", "width"]
    df_width_lookup = df_opt_output_with_store_cluster_id.select(*dims).dropDuplicates()
    dup_check(df_width_lookup, ["item_no"])

    df = df.join(df_width_lookup, "item_no", "left")

    return df


def add_cols_from_concat_summary_legal_breaks(
    pdf: pd.DataFrame,
    df_summary_legal_breaks: SparkDataFrame,
):
    dims = ["Section_Master", "Store_Physical_Location_No"]
    dup_check(df_summary_legal_breaks, dims)

    dims = ["store_physical_location_id", "item_no"]
    dup_check_pd(pdf, dims)

    # these columns will be added to opt output from the
    # concat_summary_legal_breaks dataset by joining on
    # ["Section_Master", "Store_Physical_Location_No"]
    cols_to_add = [
        ("Section_Master", "section_master", T.StringType()),
        ("Store_Physical_Location_No", "store_physical_location_id", T.StringType()),
        ("Cur_Theoretic_Lin_Space", "cur_theoretic_lin_space", T.FloatType()),
        ("Opt_Legal_Breaks", "opt_legal_breaks", T.FloatType()),
        ("Local_Item_Width", "local_item_width", T.FloatType()),
        ("Cur_Legal_Breaks", "cur_legal_breaks", T.FloatType()),
        ("Cur_Section_Width_Deviation", "cur_section_width_deviation", T.FloatType()),
        ("No_Of_Shelves", "no_of_shelves", T.FloatType()),
    ]

    df_lookup = apply_data_definition(
        df=df_summary_legal_breaks,
        data_contract=cols_to_add,
        drop_if_not_in_data_contract=True,
    )

    pdf_lookup = df_lookup.toPandas()

    # join in the required information
    dims = ["section_master", "store_physical_location_id"]
    dup_check_pd(pdf_lookup, dims)
    pdf_result = pdf.merge(right=pdf_lookup, how="left", on=dims)

    # check to ensure join is clean
    msg = "Bad lookup of data from concat_summary_legal_breaks dataset"
    assert not pdf_result["opt_legal_breaks"].isnull().any(), msg

    return pdf_result
