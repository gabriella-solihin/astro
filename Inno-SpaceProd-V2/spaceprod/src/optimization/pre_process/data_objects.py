from typing import Dict, List

import pandas as pd

from spaceprod.src.optimization.data_objects import DataContainer


class RawDataRegionBannerDeptStepOne(DataContainer):
    location_df: pd.DataFrame
    merged_clusters_df: pd.DataFrame
    elasticity_curves_sales_df: pd.DataFrame
    elasticity_curves_margin_df: pd.DataFrame
    shelve_space_df: pd.DataFrame
    product_df: pd.DataFrame
    pdf_bay_data: pd.DataFrame
    pdf_supplier_item_mapping: pd.DataFrame
    pdf_own_brands_item_mapping: pd.DataFrame
    pdf_localized_space_request: pd.DataFrame
    pdf_default_localized_space: pd.DataFrame
    pdf_supplier_and_own_brands_request: pd.DataFrame
    pdf_sales_penetration: pd.DataFrame
    pdf_margin_penetration: pd.DataFrame
    pdf_items_held_constant: pd.DataFrame


class RawDataRegionBannerDeptStepOneRerun(RawDataRegionBannerDeptStepOne):
    pass


class RawDataRegionBannerDeptStepTwo(DataContainer):
    merged_clusters_df: pd.DataFrame
    cluster_lookup_from_store_phys: Dict[str, str]
    location_df: pd.DataFrame
    shelve_space_df: pd.DataFrame
    shelve_space_df_original: pd.DataFrame
    macro_adjusted_section_length: List[List[str]]
    elasticity_curves: pd.DataFrame
    list_of_reruns_df: pd.DataFrame
    product_info_original: pd.DataFrame
    product_info: pd.DataFrame
    store_category_dims: pd.DataFrame
    pdf_bay_data: pd.DataFrame
    pdf_item_counts: pd.DataFrame
    supplier_own_brands_mapping: pd.DataFrame
    supplier_own_brands_request: pd.DataFrame
    supplier_item_mapping: pd.DataFrame
    pdf_sales_penetration: pd.DataFrame
    pdf_margin_penetration: pd.DataFrame


class RawDataRegionBannerDeptStepTwoRerun(RawDataRegionBannerDeptStepTwo):
    pass


class RawDataRegionBannerDeptStore(DataContainer):
    cluster_lookup_from_store_phys: Dict[str, str]
    merged_clusters_df: pd.DataFrame
    elasticity_curves: pd.DataFrame
    product_info: pd.DataFrame
    location_df: pd.DataFrame
    product_info_original: pd.DataFrame
    shelve_space_df_original: pd.DataFrame
    store_category_dims: pd.DataFrame
    shelve_space_df: pd.DataFrame
    elasticity_curves: pd.DataFrame
    product_info: pd.DataFrame
    pdf_bay_data: pd.DataFrame
    pdf_item_counts: pd.DataFrame
    supplier_own_brands_mapping: pd.DataFrame
    supplier_own_brands_request: pd.DataFrame
    supplier_item_mapping: pd.DataFrame
    pdf_sales_penetration: pd.DataFrame
    pdf_margin_penetration: pd.DataFrame


class RawDataRegionBannerDeptStoreRerun(RawDataRegionBannerDeptStore):
    pass


class RawDataListOfReruns(DataContainer):
    pdf_list_of_reruns: pd.DataFrame
