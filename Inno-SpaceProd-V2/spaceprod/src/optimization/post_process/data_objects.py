import pandas as pd

from spaceprod.src.optimization.data_objects import DataContainer


class DataModellingResultsProcessingPerRegBanDept(DataContainer):
    item_output: pd.DataFrame
    summary_per_store_and_cat: pd.DataFrame
    summary_facing_changes: pd.DataFrame
    summary_legal_breaks: pd.DataFrame
    supplier_space_analysis: pd.DataFrame


class DataModellingResultsProcessingPerRegBanDeptRerun(
    DataModellingResultsProcessingPerRegBanDept
):
    pass


class DataModellingConcatenatedResults(DataContainer):
    item_output_master: pd.DataFrame
    summary_per_store_master: pd.DataFrame
    summary_facing_master: pd.DataFrame
    summary_legal_breaks_master: pd.DataFrame
    supplier_space_analysis_summary: pd.DataFrame
    region_banner_dept_store_summary: pd.DataFrame
    region_banner_dept_summary: pd.DataFrame
    region_banner_summary: pd.DataFrame


class DataModellingConcatenatedResultsRerun(DataModellingConcatenatedResults):
    pass
