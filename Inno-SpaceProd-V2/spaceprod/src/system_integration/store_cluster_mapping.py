from typing import Tuple

import numpy as np
import pandas as pd
from inno_utils.loggers.log import log

from spaceprod.src.system_integration import data_contracts as dc
from spaceprod.src.utils import process_location
from spaceprod.utils.data_helpers import backup_on_blob
from spaceprod.utils.data_transformation import (
    apply_data_definition,
    apply_data_definition_pd,
)
from spaceprod.utils.imports import SparkDataFrame
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.validation import dup_check, dup_check_pd


def get_door_numbers_by_store_dept(
    df_summary_legal_breaks: SparkDataFrame,
) -> pd.DataFrame:
    """
    TODO
    Parameters
    ----------
    df_summary_legal_breaks :

    Returns
    -------

    """

    pdf = df_summary_legal_breaks.toPandas()

    dims = ["Section_Master", "Store_Physical_Location_No"]
    dup_check_pd(pdf, dims)

    # type conversion
    pdf["Opt_Legal_Breaks"] = pdf["Opt_Legal_Breaks"].astype(float)

    # some renaming of columns for downstream compatibility
    ren = {
        "Store_Physical_Location_No": "store_physical_location_id",
        "Opt_Legal_Breaks": "category_size",
        "Department": "department",
        "Section_Master": "section_master",
    }

    ren_dpt = {
        "Store_Physical_Location_No": "store_physical_location_id",
        "Opt_Legal_Breaks": "department_size",
        "Department": "department",
    }

    pdf_doors: pd.DataFrame = (
        pdf.groupby(["Store_Physical_Location_No", "Department", "Section_Master"])
        .agg({"Opt_Legal_Breaks": "sum"})
        .reset_index(drop=False)
        .rename(ren, axis=1, errors="raise")
    )

    pdf_doors_dpt_size: pd.DataFrame = (
        pdf.groupby(["Store_Physical_Location_No", "Department"])
        .agg({"Opt_Legal_Breaks": "sum"})
        .reset_index(drop=False)
        .rename(ren_dpt, axis=1, errors="raise")
    )

    pdf_result = pdf_doors.merge(
        pdf_doors_dpt_size, "left", ["store_physical_location_id", "department"]
    )

    return pdf_result


def create_frozen_store_sizes(
    df_region_banner_dept_summary: SparkDataFrame,
) -> pd.DataFrame:
    """
    Creates dataset containing store list in scope for system files.

    Parameters
    ----------
    df_summary_legal_breaks : output of the optimization
    df_region_banner_dept_summary : output of the optimization

    Returns
    -------
    a dataset containing store list in scope for system files
    """

    pdf = df_region_banner_dept_summary.toPandas()

    col_rename_mapping = {
        "Region_Desc": "region",
        "Banner": "banner",
        "Department": "department",
        "M_Cluster": "segmentation_cluster",
        "Store_Physical_Location_No": "store_physical_location_id",
    }

    keep_cols = list(col_rename_mapping.keys())
    pdf = pdf.loc[:, keep_cols].rename(col_rename_mapping, axis="columns")

    return pdf


def add_col_department_size(pdf: pd.DataFrame, pdf_doors: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.merge(
        right=pdf_doors,
        on=["store_physical_location_id", "department"],
        how="inner",
    )

    return pdf


def add_col_size_uom(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Adds the size_uom information, i.e. the unit of measure
    which is department-driven, can be either be "D" for doors
    or "F" for feet
    """

    mapping_size_uom = {
        "Frozen": "D",
        "Centre Store": "F",
        "Non Food HABA": "F",
        "Impulse": "F",
        "Dairy": "F",
        "Non Food GM": "F",
    }

    col_size_uom = pdf["department"].map(mapping_size_uom)

    msg = f"Bad mapping for 'size_uom', some departments are not mapped"
    assert not col_size_uom.isnull().any(), msg

    pdf["size_uom"] = col_size_uom

    return pdf


def update_section_master_size(pdf: pd.DataFrame):
    """
    adding 'department_size' as a product of # of breaks
    and the increment width.
    For departments that use "doors" ("D"), we only take the # of breaks
    """

    mask = pdf["size_uom"] == "D"
    col_width = pdf["legal_increment_width"]
    # legal_increment_width is in inches and dividing the inches by 12 to get ft
    col_sm_size_other = (pdf["category_size"] * col_width) / 12
    col_sm_size_d = pdf["category_size"]
    col_sm_size = np.where(mask, col_sm_size_d, col_sm_size_other)
    pdf["category_size"] = col_sm_size
    return pdf


def add_col_store_no_store_cluster_mapping(
    pdf: pd.DataFrame, df_location: SparkDataFrame
) -> pd.DataFrame:
    """
    Adds 'store_no' column to the store_cluster_mapping output
    Parameters
    ----------
    pdf : pd.DataFrame
        store_cluster_mapping dataset

    df_location : SparkDataFrame
        external location data

    Returns
    -------
    store_cluster_mapping dataset with 'store_no'
    """

    # shortcut
    df_loc = df_location

    # dedup to make sure we
    cols = ["STORE_NO", "STORE_PHYSICAL_LOCATION_NO"]
    df_loc = process_location(df_loc, "STORE_PHYSICAL_LOCATION_NO").select(*cols)

    # TODO: there was a weird behaviour in spark where the .toPandas()
    #  conversion two lines below was replacing values in the data and making
    #  all rows in data the same. it was not possible to reproduce this over
    #  databricks-connect and only occured on DevOps Pipelines, the below
    #  backup_on_blob seems to be a workaround for this
    df_loc = backup_on_blob(spark, df_loc)

    dup_check(df_loc, ["STORE_PHYSICAL_LOCATION_NO"])
    pdf_loc = df_loc.toPandas()

    # rename the column name for compatibility
    ren = {
        "STORE_PHYSICAL_LOCATION_NO": "store_physical_location_id",
        "STORE_NO": "store_no",
    }

    pdf_loc = pdf_loc.rename(columns=ren, errors="raise")

    # TODO: this should not be here since we dedup the loc as part of
    #  process_location(...)
    pdf_loc = pdf_loc.drop_duplicates(subset=["store_physical_location_id"])

    # ensures there is only 1 'store_physical_location_id' per 'store_no'
    dup_check_pd(pdf_loc, ["store_physical_location_id"])

    # do a dup check at the beginning and the end on the main data to ensure
    # that the join did not cause explosion. I.e. an explosion can happen
    # if we ever have more than 1 store_no for each store_physical_location_id
    dup_check_pd(pdf, ["store_physical_location_id", "store_cluster_id"])

    # perform the join to lookup 'store_no'
    pdf = pdf.merge(right=pdf_loc, on="store_physical_location_id", how="left")

    # do post-merge dup check to make sure no explosion happened
    # (see comment above)
    dup_check_pd(pdf, ["store_physical_location_id", "store_cluster_id"])

    pdf_check = pdf[pdf["store_no"].isnull()]
    l_stores_all = pdf_loc["store_physical_location_id"].unique().tolist()
    l_stores_mis = pdf_check["store_physical_location_id"].unique().tolist()

    msg = f"""
    \nBad store_no lookup for store_cluster_mapping. 
    Here is the non-match slice:
    \n{pdf_check}
    \nStores available for lookup: {sorted(l_stores_all)}
    \nStores missing: {sorted(l_stores_mis)}
    """

    assert len(pdf_check) == 0, msg

    return pdf


def create_store_cluster_mapping_skeleton(
    df_opt_output_with_store_cluster_id: SparkDataFrame,
):
    """
    Creates the skeleton for the store_cluster_mapping output,
    i.e. only the dimensional data + some attributes

    Parameters
    ----------
    df_opt_output_with_store_cluster_id : pd.DataFrame
        dataset containing store list in scope


    Returns
    -------
    skeleton dataset for the store/cluster mapping system file
    """

    pdf = df_opt_output_with_store_cluster_id.toPandas()

    cols_mapping = [
        ("store_physical_location_id", "store_physical_location_id", str),
        ("store_cluster_id", "store_cluster_id", str),
        ("region", "region", str),
        ("banner", "banner", str),
        ("department", "department", str),
        ("m_cluster", "segmentation_cluster", str),
        ("size_uom", "size_uom", str),
        ("section_master", "category", str),
        ("category_size", "category_size", int),
    ]

    pdf = apply_data_definition_pd(
        pdf=pdf,
        data_contract=cols_mapping,
        drop_if_not_in_data_contract=True,
    )

    pdf = pdf.drop_duplicates()
    dims = ["store_cluster_id", "store_physical_location_id"]
    dup_check_pd(pdf, dims)

    return pdf


def add_col_store_cluster_id(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Adds column that represents the 'store_cluster_id'
    TODO: should be replaced with proper formula for store_cluster_id

    Parameters
    ----------
    pdf : pd.DataFrame
        store_cluster_mapping dataset

    Returns
    -------
    store_cluster_mapping dataset with store_cluster_id column
    """

    # TODO: this is purely arbitrary, please change this mapping if needed
    map_region = {
        "ontario": "ONT",
        "west": "WES",
        "quebec": "QUE",
        "atlantic": "ATL",
    }

    # TODO: this is purely arbitrary, please change this mapping if needed
    map_banners = {
        "CONVENIENCE": "CON",
        "OTHER": "OTH",
        "IGA": "IGA",
        "SOBEYS": "SBY",
        "FOODLAND": "FDL",
        "THRIFTY FOODS": "THF",
        "FRESH CO": "FCO",
        "RACHELLE-BERY": "RLB",
        "BONICHOIX": "BCX",
        "TRADITION": "TRD",
        "SAFEWAY": "SFY",
    }

    # TODO: this is purely arbitrary, please change this mapping if needed
    map_depts = {
        "Frozen": "FRZ",
        "Centre Store": "CEN",
        "Impulse": "IMP",
        "Dairy": "DAI",
        "Non Food": "NFD",
        "Out of Scope": "OOS",
        "Out of Scope ": "OOS",
    }

    pdf["store_cluster_id"] = (
        pdf.replace({"region": map_region})["region"]
        + "-"
        + pdf.replace({"banner": map_banners})["banner"]
        + "-"
        + pdf["m_cluster"]
        + "-"
        + pdf.replace({"department": map_depts})["department"]
        + "-"
        + pdf["section_master"]
        + "-"
        # TODO: for the 'department_size' consider rounding before casting to int
        + pdf["category_size"].astype(int).astype(str)
        + pdf["size_uom"]
    )
    return pdf


def add_col_segmentation_cluster_desc(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Adds column that represents the 'segmentation_cluster_desc'
    TODO: should be replaced with proper formula for store_cluster_id

    Parameters
    ----------
    pdf : pd.DataFrame
        store_cluster_mapping dataset

    Returns
    -------
    store_cluster_mapping dataset with store_cluster_id column
    """

    # add in segmentation_cluster description (dummy for now)
    pdf["segmentation_cluster_desc"] = (
        "some description for " + pdf["segmentation_cluster"]
    )

    return pdf


def prepare_data_for_output_store_cluster_mapping(
    pdf: pd.DataFrame,
) -> Tuple[SparkDataFrame, SparkDataFrame]:
    """
    Prepares the store_cluster_mapping dataset for the final output by:
     - selecting only relevant columns
     - performing final type conversion

    Parameters
    ----------
    pdf : pd.DataFrame
        store_cluster_mapping dataset

    pdf_frozen_store_sizes : pd.DataFrame
        dataset containing store list in scope

    Returns
    -------
    final store_cluster_mapping dataset and list of stores in scope
    """

    # fix column order
    cols_reqd = [
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
        "store_cluster_id",
        "expected_publish_date",
        "run_name",
        "run_id",
        "run_datetime",
    ]

    store_cluster_mapping = pdf[cols_reqd]

    df_store_cluster_mapping = spark.createDataFrame(store_cluster_mapping)

    cols = [
        (
            x.name,
            x.name,
            x.dataType,
        )
        for x in dc.store_cluster_mapping.schema
    ]

    df_store_cluster_mapping = apply_data_definition(
        df=df_store_cluster_mapping,
        data_contract=cols,
    )

    return df_store_cluster_mapping
