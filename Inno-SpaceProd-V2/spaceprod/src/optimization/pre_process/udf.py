import traceback
from typing import Dict, List, Callable

import pandas as pd

from inno_utils.loggers import log
from spaceprod.src.optimization.data_objects import (
    DataContainer,
    DataObjectScopeGeneral,
    DataObjectScopeSpecific,
    RawDataGeneral,
)
from spaceprod.src.optimization.pre_process.data_objects import (
    RawDataRegionBannerDeptStepOne,
    RawDataRegionBannerDeptStepOneRerun,
    RawDataRegionBannerDeptStepTwo,
    RawDataRegionBannerDeptStepTwoRerun,
    RawDataRegionBannerDeptStore,
    RawDataRegionBannerDeptStoreRerun,
)
from spaceprod.src.optimization.pre_process.helpers_data_ingest import (
    RawDataContainer,
)
from spaceprod.src.optimization.pre_process.helpers_data_preparation import (
    ProcessedDataContainer,
    add_maximum_allocateable_facings_from_POG_to_store_cat_dims,
    prep_elasticity_curve_df,
    prep_location_data,
    prep_merged_cluster_data,
    prep_store_category_dimensions,
    prepare_product_info_and_dimensions,
    prepare_shelve_space_df,
    section_length_macro_micro_factor_calculation,
    add_cur_fac_width_to_shelve,
    add_new_assortment_to_items_in_product_info,
    add_legal_section_breaks_to_store_cat_dims,
    correct_wrong_linear_space_per_break,
    filter_constraint_mapping_to_relevant_items,
    prepare_supplier_and_own_brands_to_item_mapping,
    add_local_item_width_to_store_cat_dims,
    prep_supplier_own_brands_request,
    prepare_item_counts,
)
from spaceprod.src.optimization.pre_process.helpers_rerun import (
    filter_data_preparation_files_by_rerun_sections,
)
from spaceprod.utils.data_helpers import (
    backup_on_blob,
    filter_and_assert_not_empty,
    filter_df_for_container,
)
from spaceprod.utils.imports import F, SparkDataFrame, T
from spaceprod.utils.names import get_col_names
from spaceprod.utils.space_context.spark import spark


def generate_udf_pre_proc_region_banner_dept_step_one(
    path_opt_data_containers: str,
    rerun: bool,
) -> Callable:
    """
    Generates the UDF function that is used to produce output of the
    task_opt_pre_proc_region_banner_dept_step_one task which is part of the
    preprocessing step for optimization.

    Parameters
    ----------
    path_opt_data_containers: str
        generic path within the run folder that contains paths

    rerun : bool
        whether or not to run in a 'rerun' mode for margin

    Returns
    -------
    UDF function to run
    """

    schema_out = T.StructType(
        [
            T.StructField("REGION", T.StringType(), True),
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("DEPT", T.StringType(), True),
            T.StructField("PATH_PRE_PROC_RAW_DATA_STEP_ONE", T.StringType(), True),
        ]
    )

    @F.pandas_udf(schema_out, F.PandasUDFType.GROUPED_MAP)
    def udf(pdf: pd.DataFrame):

        n = get_col_names()

        region = pdf["REGION"].tolist()[0]
        banner = pdf["BANNER"].tolist()[0]
        dept = pdf["DEPT"].tolist()[0]
        # region="ontario";banner="SOBEYS";dept="Frozen"

        from spaceprod.src.optimization.data_objects import DataContainer

        data_container = DataContainer(scope=DataObjectScopeGeneral())
        raw_data = data_container.read(path_opt_data_containers, "RawDataGeneral")

        log.info(
            f"Ingested and splitting up raw data for region banner department {region} {banner} {dept}"
        )

        raw_data_by_reg_banner_dept = RawDataContainer()

        # location data assignment and filtering
        raw_data_by_reg_banner_dept.location_df = raw_data.location

        raw_data_by_reg_banner_dept.location_df = filter_and_assert_not_empty(
            raw_data_by_reg_banner_dept.location_df,
            "location_df",
            n.F_REGION_DESC,
            region,
        )

        raw_data_by_reg_banner_dept.location_df = filter_and_assert_not_empty(
            raw_data_by_reg_banner_dept.location_df,
            "location_df",
            n.F_BANNER,
            banner,
        )

        # merged store cluster data assignment and filtering
        raw_data_by_reg_banner_dept.merged_clusters_df = raw_data.merged_clusters_df
        raw_data_by_reg_banner_dept.merged_clusters_df = filter_and_assert_not_empty(
            raw_data_by_reg_banner_dept.merged_clusters_df,
            "merged_clusters_df",
            n.F_REGION_DESC,
            region,
        )
        raw_data_by_reg_banner_dept.merged_clusters_df = filter_and_assert_not_empty(
            raw_data_by_reg_banner_dept.merged_clusters_df,
            "merged_clusters_df",
            n.F_BANNER,
            banner,
        )

        # elasticity sales assignment and filtering
        raw_data_by_reg_banner_dept.elasticity_curves_sales_df = (
            raw_data.elasticity_curves_sales_df
        )
        raw_data_by_reg_banner_dept.elasticity_curves_sales_df = (
            filter_and_assert_not_empty(
                raw_data_by_reg_banner_dept.elasticity_curves_sales_df,
                "elasticity_curves_sales_df",
                n.F_REGION_DESC,
                region,
            )
        )
        raw_data_by_reg_banner_dept.elasticity_curves_sales_df = (
            filter_and_assert_not_empty(
                raw_data_by_reg_banner_dept.elasticity_curves_sales_df,
                "elasticity_curves_sales_df",
                n.F_BANNER,
                banner,
            )
        )
        raw_data_by_reg_banner_dept.elasticity_curves_sales_df = (
            filter_and_assert_not_empty(
                raw_data_by_reg_banner_dept.elasticity_curves_sales_df,
                "elasticity_curves_sales_df",
                n.F_DEPARTMENT,
                dept,
            )
        )

        # elasticity margin assignment and filtering
        raw_data_by_reg_banner_dept.elasticity_curves_margin_df = (
            raw_data.elasticity_curves_margin_df
        )
        raw_data_by_reg_banner_dept.elasticity_curves_margin_df = (
            filter_and_assert_not_empty(
                raw_data_by_reg_banner_dept.elasticity_curves_margin_df,
                "elasticity_curves_margin_df",
                n.F_REGION_DESC,
                region,
            )
        )
        raw_data_by_reg_banner_dept.elasticity_curves_margin_df = (
            filter_and_assert_not_empty(
                raw_data_by_reg_banner_dept.elasticity_curves_margin_df,
                "elasticity_curves_margin_df",
                n.F_BANNER,
                banner,
            )
        )
        raw_data_by_reg_banner_dept.elasticity_curves_margin_df = (
            filter_and_assert_not_empty(
                raw_data_by_reg_banner_dept.elasticity_curves_margin_df,
                "elasticity_curves_margin_df",
                n.F_DEPARTMENT,
                dept,
            )
        )

        # bay data pre index for unit proportions assignment and filtering
        raw_data_by_reg_banner_dept.bay_data = raw_data.pdf_bay_data
        raw_data_by_reg_banner_dept.bay_data = filter_and_assert_not_empty(
            raw_data_by_reg_banner_dept.bay_data,
            "bay_data",
            n.F_REGION_DESC,
            region,
        )
        raw_data_by_reg_banner_dept.bay_data = filter_and_assert_not_empty(
            raw_data_by_reg_banner_dept.bay_data,
            "bay_data",
            n.F_BANNER,
            banner,
        )
        raw_data_by_reg_banner_dept.bay_data = filter_and_assert_not_empty(
            raw_data_by_reg_banner_dept.bay_data,
            "bay_data",
            n.F_DEPARTMENT,
            dept,
        )

        # shelve space (planogram / POG) assignment and filtering
        raw_data_by_reg_banner_dept.shelve_space_df = raw_data.shelve_space_df
        # TODO do we need to filter down? Then we need to prep columns (e.g. filter on store phys from location etc)

        # product_df assignment and filtering
        raw_data_by_reg_banner_dept.product_df = raw_data.product_df
        # TODO do we need to filter down? Then we need to prep columns (e.g. filter on POG?)

        # constraints
        raw_data_by_reg_banner_dept.default_localized_space = (
            raw_data.pdf_default_localized_space
        )
        # we don't assert empty df because default space could be empty for a specific reg ban
        raw_data_by_reg_banner_dept.default_localized_space = filter_df_for_container(
            raw_data_by_reg_banner_dept.default_localized_space,
            "default_localized_space",
            n.F_REGION_DESC,
            region,
        )
        # we don't assert empty df because default space could be empty for a specific reg ban
        raw_data_by_reg_banner_dept.default_localized_space = filter_df_for_container(
            raw_data_by_reg_banner_dept.default_localized_space,
            "default_localized_space",
            n.F_BANNER,
            banner,
        )

        # supplier request
        raw_data_by_reg_banner_dept.supplier_and_own_brands_request = (
            raw_data.pdf_supplier_and_own_brands_request
        )
        raw_data_by_reg_banner_dept.supplier_and_own_brands_request = (
            filter_and_assert_not_empty(
                raw_data_by_reg_banner_dept.supplier_and_own_brands_request,
                "supplier_and_own_brands_request",
                n.F_REGION_DESC,
                region,
            )
        )
        raw_data_by_reg_banner_dept.supplier_and_own_brands_request = (
            filter_and_assert_not_empty(
                raw_data_by_reg_banner_dept.supplier_and_own_brands_request,
                "supplier_and_own_brands_request",
                n.F_BANNER,
                banner,
            )
        )

        # local space request
        raw_data_by_reg_banner_dept.localized_space_request = (
            raw_data.pdf_localized_space_request
        )
        # we don't assert empty df because default space could be empty for a specific reg ban
        raw_data_by_reg_banner_dept.localized_space_request = filter_df_for_container(
            raw_data_by_reg_banner_dept.localized_space_request,
            "localized_space_request",
            n.F_REGION_DESC,
            region,
        )
        # we don't assert empty df because default space could be empty for a specific reg ban
        raw_data_by_reg_banner_dept.localized_space_request = filter_df_for_container(
            raw_data_by_reg_banner_dept.localized_space_request,
            "localized_space_request",
            n.F_BANNER,
            banner,
        )

        # sales penetration
        raw_data_by_reg_banner_dept.pdf_sales_penetration = (
            raw_data.pdf_sales_penetration
        )
        raw_data_by_reg_banner_dept.pdf_sales_penetration = filter_df_for_container(
            raw_data_by_reg_banner_dept.pdf_sales_penetration,
            "sales_penetration_df",
            n.F_REGION_DESC,
            region,
        )
        raw_data_by_reg_banner_dept.pdf_sales_penetration = filter_df_for_container(
            raw_data_by_reg_banner_dept.pdf_sales_penetration,
            "sales_penetration_df",
            n.F_BANNER,
            banner,
        )
        raw_data_by_reg_banner_dept.pdf_sales_penetration = filter_df_for_container(
            raw_data_by_reg_banner_dept.pdf_sales_penetration,
            "sales_penetration_df",
            n.F_DEPARTMENT,
            dept,
        )

        # margin penetration
        raw_data_by_reg_banner_dept.pdf_margin_penetration = (
            raw_data.pdf_margin_penetration
        )
        raw_data_by_reg_banner_dept.pdf_margin_penetration = filter_df_for_container(
            raw_data_by_reg_banner_dept.pdf_margin_penetration,
            "margin_penetration_df",
            n.F_REGION_DESC,
            region,
        )
        raw_data_by_reg_banner_dept.pdf_margin_penetration = filter_df_for_container(
            raw_data_by_reg_banner_dept.pdf_margin_penetration,
            "margin_penetration_df",
            n.F_BANNER,
            banner,
        )
        raw_data_by_reg_banner_dept.pdf_margin_penetration = filter_df_for_container(
            raw_data_by_reg_banner_dept.pdf_margin_penetration,
            "margin_penetration_df",
            n.F_DEPARTMENT,
            dept,
        )

        # items held constant per region banner #can be empty
        raw_data_by_reg_banner_dept.pdf_items_held_constant = (
            raw_data.pdf_items_held_constant
        )
        raw_data_by_reg_banner_dept.pdf_items_held_constant = filter_df_for_container(
            raw_data_by_reg_banner_dept.pdf_items_held_constant,
            "pdf_items_held_constant",
            n.F_REGION_DESC,
            region,
        )
        raw_data_by_reg_banner_dept.pdf_items_held_constant = filter_df_for_container(
            raw_data_by_reg_banner_dept.pdf_items_held_constant,
            "pdf_items_held_constant",
            n.F_BANNER,
            banner,
        )
        raw_data_by_reg_banner_dept.pdf_items_held_constant = filter_df_for_container(
            raw_data_by_reg_banner_dept.pdf_items_held_constant,
            "pdf_items_held_constant",
            n.F_DEPARTMENT,
            dept,
        )

        raw_data_by_reg_banner_dept.supplier_item_mapping = (
            raw_data.pdf_supplier_item_mapping
        )
        raw_data_by_reg_banner_dept.own_brands_item_mapping = (
            raw_data.pdf_own_brands_item_mapping
        )

        # creating an object that tells our data container for which
        # scope we want this data to be (to be saved)
        scope = DataObjectScopeSpecific(
            region=region,
            banner=banner,
            dept=dept,
        )

        DataContainer = (
            RawDataRegionBannerDeptStepOneRerun
            if rerun
            else RawDataRegionBannerDeptStepOne
        )

        data_raw_region_banner_dept = DataContainer(
            location_df=raw_data_by_reg_banner_dept.location_df,
            merged_clusters_df=raw_data_by_reg_banner_dept.merged_clusters_df,
            elasticity_curves_sales_df=raw_data_by_reg_banner_dept.elasticity_curves_sales_df,
            elasticity_curves_margin_df=raw_data_by_reg_banner_dept.elasticity_curves_margin_df,
            shelve_space_df=raw_data_by_reg_banner_dept.shelve_space_df,
            product_df=raw_data_by_reg_banner_dept.product_df,
            pdf_bay_data=raw_data_by_reg_banner_dept.bay_data,
            pdf_supplier_item_mapping=raw_data_by_reg_banner_dept.supplier_item_mapping,
            pdf_own_brands_item_mapping=raw_data_by_reg_banner_dept.own_brands_item_mapping,
            pdf_localized_space_request=raw_data_by_reg_banner_dept.localized_space_request,
            pdf_default_localized_space=raw_data_by_reg_banner_dept.default_localized_space,
            pdf_supplier_and_own_brands_request=raw_data_by_reg_banner_dept.supplier_and_own_brands_request,
            pdf_sales_penetration=raw_data_by_reg_banner_dept.pdf_sales_penetration,
            pdf_margin_penetration=raw_data_by_reg_banner_dept.pdf_margin_penetration,
            pdf_items_held_constant=raw_data_by_reg_banner_dept.pdf_items_held_constant,
            scope=scope,
        )

        data_raw_region_banner_dept.save(path_opt_data_containers)

        pdf_result = pd.DataFrame(
            {
                "REGION": [region],
                "BANNER": [banner],
                "DEPT": [dept],
                "PATH_PRE_PROC_RAW_DATA_STEP_ONE": [data_raw_region_banner_dept.path],
            }
        )

        return pdf_result

    return udf


def generate_udf_pre_proc_region_banner_dept_step_two(
    path_opt_data_containers: str,
    dependent_var: str,
    max_facings: int,
    use_macro_section_length: bool,
    enable_margin_reruns: bool,
    rerun: bool,
    hold_items_constant_from_file: bool,
    section_master_excluded: List[str],
    section_length_lower_deviation: float,
    section_length_upper_deviation: float,
    difference_of_facings_to_allocate_from_max_in_POG: int,
    legal_section_break_increments: Dict[str, int],
    legal_section_break_dict,
    minimum_shelves_assumption: int,
    possible_number_of_shelves_min: int,
    possible_number_of_shelves_max: int,
    overwrite_at_min_shelve_increment: bool,
    enable_localized_space: bool,
    overwrite_local_space_percentage: float,
    filter_for_test_negotiations: bool,
    run_on_theoretic_space: bool,
    facing_percentile: float,
):
    """
    Generates and runs the UDF function that is used to produce output of the
    task_opt_pre_proc_region_banner_dept_step_two task which is part of the
    pre-processing step for optimization.

    Parameters
    ----------
    dependent_var : str
        sales or margin

    max_facings : int
        TODO

    use_macro_section_length : bool
        if this is set to true we use the macro allocated space per
        section in the micro opt instead of the current POG section length
        equivalent space

    enable_margin_reruns : bool
        if margin reruns should be enabled we save sales space for margin
        categories and use that as constraint to run those sections on margin
        again

    item_numbers_for_constant_treatment : bool

    section_master_excluded : List[str]
        TODO

    path_opt_data_containers: str
        generic path within the run folder that contains paths

    rerun : bool
        whether or not to run in a 'rerun' mode for margin

    Returns
    -------
    the UDF callable that needs to run in parallel for each dimension
    """

    schema_out = T.StructType(
        [
            T.StructField("REGION", T.StringType(), True),
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("DEPT", T.StringType(), True),
            T.StructField("PATH_PRE_PROC_RAW_DATA_STEP_TWO", T.StringType(), True),
            T.StructField("STORE_LIST", T.ArrayType(T.StringType()), True),
        ]
    )

    @F.pandas_udf(schema_out, F.PandasUDFType.GROUPED_MAP)
    def udf(pdf):

        region = pdf["REGION"].tolist()[0]
        banner = pdf["BANNER"].tolist()[0]
        dept = pdf["DEPT"].tolist()[0]

        # region='ontario'
        # banner='SOBEYS'
        # dept='Frozen'

        # get names dict and pass into functions for unified column names
        n = get_col_names()

        log.info(f"data preparation for {region} {banner} and department {dept}")

        scope = DataObjectScopeSpecific(region=region, banner=banner, dept=dept)
        data_container = DataContainer(scope=scope)

        container_name = (
            "RawDataRegionBannerDeptStepOneRerun"
            if rerun
            else "RawDataRegionBannerDeptStepOne"
        )

        raw_data = data_container.read(
            path_parent=path_opt_data_containers,
            container_name=container_name,
        )

        processed_data = ProcessedDataContainer()

        # this prepares the cluster file
        (
            processed_data.merged_clusters_df,
            processed_data.cluster_lookup_from_store_phys,
        ) = prep_merged_cluster_data(n, raw_data.merged_clusters_df)

        raw_data.location_df = raw_data.location_df.rename(
            columns={"STORE_NO": "Store_No"}
        )
        raw_data.shelve_space_df = raw_data.shelve_space_df.rename(
            columns={
                "RELEASE_DATE": "Release_Date",
                "LENGTH_SECTION_INCHES": "Length_Section_Inches",
            }
        )

        processed_data.location_df = prep_location_data(n, raw_data.location_df)

        processed_data.shelve_space_df = prepare_shelve_space_df(
            n,
            raw_data.shelve_space_df,
            processed_data.location_df,
        )

        # prepare item counts which are used in the optimization to constrain by unit proportion
        processed_data.pdf_item_counts = prepare_item_counts(
            n=n,
            bay_data=raw_data.pdf_bay_data,
            cluster_assignment=processed_data.merged_clusters_df,
        )

        # this below is done so we can pull in item width from items that haven't been assorted before but now through opt
        processed_data.shelve_space_df_original = processed_data.shelve_space_df

        # following only needed if macro section length is used in micro
        if use_macro_section_length:
            # TODO needs to be changed for integrated
            processed_data.macro_adjusted_section_length = (
                section_length_macro_micro_factor_calculation(
                    n=n,
                    current_facings_width=processed_data.shelve_space_df,
                    macro_space_allocation=raw_data.macro_section_length,
                )
            )
        else:
            processed_data.macro_adjusted_section_length = []

        if dependent_var == n.F_SALES:
            elasticity_curves_sales_df = prep_elasticity_curve_df(
                n=n,
                elasticity_curves=raw_data.elasticity_curves_sales_df,
                dependent_var=n.F_SALES,
                max_facings=max_facings,
            )
            processed_data.elasticity_curves = elasticity_curves_sales_df
        else:
            elasticity_curves_margin_df = prep_elasticity_curve_df(
                n=n,
                elasticity_curves=raw_data.elasticity_curves_margin_df,
                dependent_var=n.F_MARGIN,
                max_facings=max_facings,
            )
            processed_data.elasticity_curves = elasticity_curves_margin_df

        # if we do a rerun on margins we want to feed the new margin section constraint into specific stores
        if enable_margin_reruns and rerun:

            data_container = DataContainer(scope=DataObjectScopeGeneral())

            data_list_of_reruns = data_container.read(
                path_parent=path_opt_data_containers,
                container_name="RawDataListOfReruns",
            )

            # retain all stores in the cluster if we do rerun and only used a subset of stores in run for newly assorted items
            processed_data.merged_clusters_df_before_rerun_filter = (
                processed_data.merged_clusters_df
            )
            processed_data.shelve_df_before_rerun_filter = (
                processed_data.shelve_space_df
            )

            pdf_list_of_reruns = data_list_of_reruns.pdf_list_of_reruns

            (
                processed_data.elasticity_curves,
                processed_data.shelve_space_df,
                processed_data.merged_clusters_df,
                processed_data.list_of_reruns_df,
            ) = filter_data_preparation_files_by_rerun_sections(
                n=n,
                region=region,
                banner=banner,
                dept=dept,
                list_of_reruns=pdf_list_of_reruns,
                elasticity_df=processed_data.elasticity_curves,
                shelve_df=processed_data.shelve_space_df,
                cluster_info=processed_data.merged_clusters_df,
            )
        else:
            processed_data.list_of_reruns_df = pd.DataFrame()

        # the following exclusions don't go into opt but are used in results processing. henve we create master copy
        processed_data.product_info_original = raw_data.product_df

        # this prepares the df that has product information including width per facing
        if hold_items_constant_from_file:
            item_numbers_for_constant_treatment = raw_data.pdf_items_held_constant[
                n.F_ITEM_NO
            ].tolist()
        else:
            item_numbers_for_constant_treatment = []
        # outputs info by category
        processed_data.product_info = prepare_product_info_and_dimensions(
            item_numbers_for_constant_treatment=item_numbers_for_constant_treatment,
            section_master_excluded=section_master_excluded,
            product=raw_data.product_df,
            shelve=processed_data.shelve_space_df,
            elasticity_df=processed_data.elasticity_curves,
        )

        # now we make the product_info store specific and ensure we have the right assortment per cluster
        processed_data.product_info = add_cur_fac_width_to_shelve(
            n=n,
            item_df=processed_data.product_info,
            shelve=processed_data.shelve_space_df,
        )

        # this outputs space length by store and category  - Cl also included as info
        prep_store_category_dimensions_kwargs = {
            "shelve": processed_data.shelve_space_df,
            "elasticity_df": processed_data.elasticity_curves,
            "cluster_info": processed_data.merged_clusters_df,
            "product_info": processed_data.product_info,
            "macro_section_length": processed_data.macro_adjusted_section_length,
            "use_macro_section_length": use_macro_section_length,
            "section_length_lower_deviation": section_length_lower_deviation,
            "section_length_upper_deviation": section_length_upper_deviation,
        }

        if enable_margin_reruns and rerun:
            prep_store_category_dimensions_kwargs[
                "list_of_margin_reruns"
            ] = processed_data.list_of_reruns_df

            prep_store_category_dimensions_kwargs["rerun"] = True

        processed_data.store_category_dims = prep_store_category_dimensions(
            **prep_store_category_dimensions_kwargs
        )

        processed_data.store_category_dims = add_maximum_allocateable_facings_from_POG_to_store_cat_dims(
            store_category_dims=processed_data.store_category_dims,
            shelve=processed_data.shelve_space_df,
            difference_of_facings_to_allocate_from_max_in_POG=difference_of_facings_to_allocate_from_max_in_POG,
            max_facings=max_facings,
            facing_percentile=facing_percentile,
        )

        processed_data.store_category_dims = add_legal_section_breaks_to_store_cat_dims(
            store_category_dims=processed_data.store_category_dims,
            shelve=processed_data.shelve_space_df,
            legal_section_break_increments=legal_section_break_increments,
            legal_section_break_dict=legal_section_break_dict,
        )

        # correct legal breaks in section when seciton length in opt is clearly missing many items from POG
        processed_data.store_category_dims = correct_wrong_linear_space_per_break(
            store_category_dims=processed_data.store_category_dims,
            minimum_shelves_assumption=minimum_shelves_assumption,
            possible_number_of_shelves_min=possible_number_of_shelves_min,
            possible_number_of_shelves_max=possible_number_of_shelves_max,
            overwrite_at_min_shelve_increment=overwrite_at_min_shelve_increment,
            run_on_theoretic_space=run_on_theoretic_space,
        )

        # TODO add default and requested local space here
        processed_data.store_category_dims = add_local_item_width_to_store_cat_dims(
            enable_localized_space=enable_localized_space,
            store_category_dims=processed_data.store_category_dims,
            default_space=raw_data.pdf_default_localized_space,
            space_request=raw_data.pdf_localized_space_request,
            overwrite_local_space_percentage=overwrite_local_space_percentage,
        )

        if filter_for_test_negotiations:
            # DEV negotiations runs only
            inclusion_list = [
                "COFFEE INSTANT",
                "COFFEE PODS",
                "COFFEE ROAST AND GROUND",
                "NATURAL HOT BEVERAGES",
                "TEA",
                "COFFEE ALL SECTIONS COMBO",
                "Frozen Breakfast",
            ]

            processed_data.store_category_dims = processed_data.store_category_dims.loc[
                processed_data.store_category_dims[n.F_SECTION_MASTER].isin(
                    inclusion_list
                )
            ]

        if rerun:
            merged_cluster_for_newly_assorted_items = (
                processed_data.merged_clusters_df_before_rerun_filter
            )
            shelve_for_newly_assorted_items = (
                processed_data.shelve_df_before_rerun_filter
            )
        else:
            merged_cluster_for_newly_assorted_items = processed_data.merged_clusters_df
            shelve_for_newly_assorted_items = processed_data.shelve_space_df
        # now we change product_info to also include items that are not currenlty allocated but could be for opt to consider
        processed_data.product_info = add_new_assortment_to_items_in_product_info(
            product=processed_data.product_info,
            shelve=shelve_for_newly_assorted_items,
            elasticity=processed_data.elasticity_curves,
            merged_clusters=merged_cluster_for_newly_assorted_items,
            dep_var=dependent_var,
            item_numbers_for_constant_treatment=item_numbers_for_constant_treatment,
            section_master_excluded=section_master_excluded,
        )
        if rerun:
            # filter processed_data.product_info on final stores and sections only that go into rerun
            processed_data.product_info = processed_data.product_info.merge(
                processed_data.store_category_dims[
                    [n.F_STORE_PHYS_NO, n.F_SECTION_MASTER]
                ],
                how="inner",
            )

        # Prepare all constraint (supplier, own brands, local) input files by filtering them on relevant items and then combining them for constraints

        # todo check for duplicate supplier ids and OB combinations in same section and assert - set construction after useless ones filtered
        # processed_data.supplier_own_brands_request = (
        #     raw_data.pdf_supplier_and_own_brands_request[n.F_SUP_REQUEST_NEW_COLS]
        # )
        processed_data.supplier_own_brands_request = prep_supplier_own_brands_request(
            product=processed_data.product_info,
            request=raw_data.pdf_supplier_and_own_brands_request,
        )

        processed_data.supplier_item_mapping = (
            filter_constraint_mapping_to_relevant_items(
                product=processed_data.product_info,
                df=raw_data.pdf_supplier_item_mapping,
                merchant_values=processed_data.supplier_own_brands_request,
            )
        )

        (
            processed_data.supplier_own_brands_mapping,
            processed_data.supplier_own_brands_request,
        ) = prepare_supplier_and_own_brands_to_item_mapping(
            supplier_mapping=processed_data.supplier_item_mapping,
            own_brands_mapping=raw_data.pdf_own_brands_item_mapping,
            supplier_own_brands_request=processed_data.supplier_own_brands_request,
        )

        scope = DataObjectScopeSpecific(region=region, banner=banner, dept=dept)

        DataObject = (
            RawDataRegionBannerDeptStepTwoRerun
            if rerun
            else RawDataRegionBannerDeptStepTwo
        )

        data_raw_region_banner_dept_step_two = DataObject(
            merged_clusters_df=processed_data.merged_clusters_df,
            cluster_lookup_from_store_phys=processed_data.cluster_lookup_from_store_phys,
            location_df=processed_data.location_df,
            shelve_space_df=processed_data.shelve_space_df,
            shelve_space_df_original=processed_data.shelve_space_df_original,
            macro_adjusted_section_length=processed_data.macro_adjusted_section_length,
            elasticity_curves=processed_data.elasticity_curves,
            list_of_reruns_df=processed_data.list_of_reruns_df,
            product_info_original=processed_data.product_info_original,
            product_info=processed_data.product_info,
            store_category_dims=processed_data.store_category_dims,
            pdf_bay_data=raw_data.pdf_bay_data,
            pdf_item_counts=processed_data.pdf_item_counts,
            supplier_own_brands_mapping=processed_data.supplier_own_brands_mapping,
            supplier_own_brands_request=processed_data.supplier_own_brands_request,
            supplier_item_mapping=processed_data.supplier_item_mapping,
            pdf_sales_penetration=raw_data.pdf_sales_penetration,
            pdf_margin_penetration=raw_data.pdf_margin_penetration,
            scope=scope,
        )

        data_raw_region_banner_dept_step_two.save(path_opt_data_containers)

        # Create store category tuple to do the parallelization over
        store_list = list(
            set(processed_data.store_category_dims[n.F_STORE_PHYS_NO].unique())
        )

        pdf_result = pd.DataFrame(
            {
                "REGION": [region],
                "BANNER": [banner],
                "DEPT": [dept],
                "PATH_PRE_PROC_RAW_DATA_STEP_TWO": [
                    data_raw_region_banner_dept_step_two.path
                ],
                "STORE_LIST": [store_list],
            }
        )

        return pdf_result

    return udf


def generate_udf_pre_proc_region_banner_dept_store(
    path_opt_data_containers: str,
    rerun: bool,
):
    """
    Generates the UDF function that is used to produce output of the
    task_opt_pre_proc_region_banner_dept_store task which is part of the
    preprocessing step for optimization.

    Parameters
    ----------
    path_opt_data_containers: str
        generic path within the run folder that contains paths

    rerun : bool
        whether or not to run in a 'rerun' mode for margin


    Returns
    -------
    the UDF callable that needs to run in parallel for each dimension
    """
    schema_out = T.StructType(
        [
            T.StructField("REGION", T.StringType(), True),
            T.StructField("BANNER", T.StringType(), True),
            T.StructField("DEPT", T.StringType(), True),
            T.StructField("STORE", T.StringType(), True),
            T.StructField("PATH_PRE_PROC_RAW_DATA_STORE", T.StringType(), True),
        ]
    )

    def logic_to_wrap(pdf):

        region = pdf["REGION"].tolist()[0]
        banner = pdf["BANNER"].tolist()[0]
        dept = pdf["DEPT"].tolist()[0]
        store = pdf["STORE"].tolist()[0]

        # get names dict and pass into functions for unified column names
        n = get_col_names()

        # region="ontario";banner="SOBEYS";dept="Frozen";store="11414"

        scope = DataObjectScopeSpecific(region=region, banner=banner, dept=dept)
        data_container = DataContainer(scope=scope)

        container_name = (
            "RawDataRegionBannerDeptStepTwoRerun"
            if rerun
            else "RawDataRegionBannerDeptStepTwo"
        )

        processed_data = data_container.read(
            path_parent=path_opt_data_containers,
            container_name=container_name,
        )

        processed_data_by_store_category = ProcessedDataContainer()

        processed_data_by_store_category.cluster_lookup_from_store_phys = (
            processed_data.cluster_lookup_from_store_phys
        )

        processed_data_by_store_category.merged_clusters_df = (
            processed_data.merged_clusters_df
        )

        processed_data_by_store_category.shelve_space_df = (
            processed_data.shelve_space_df
        )

        processed_data_by_store_category.elasticity_curves = (
            processed_data.elasticity_curves
        )

        # currently the product infos is written every time. very small file though
        processed_data_by_store_category.product_info = processed_data.product_info

        processed_data_by_store_category.location_df = processed_data.location_df

        processed_data_by_store_category.product_info_original = (
            processed_data.product_info_original
        )

        processed_data_by_store_category.shelve_space_df_original = (
            processed_data.shelve_space_df_original
        )

        processed_data_by_store_category.store_category_dims = (
            processed_data.store_category_dims
        )

        processed_data_by_store_category.store_category_dims = (
            filter_and_assert_not_empty(
                processed_data_by_store_category.store_category_dims,
                "store_category_dims",
                n.F_STORE_PHYS_NO,
                store,
            )
        )

        # filtering the processed data by store and cat where applicable
        processed_data_by_store_category.shelve_space_df = filter_and_assert_not_empty(
            processed_data_by_store_category.shelve_space_df,
            "Shelve space df store filter",
            n.F_STORE_PHYS_NO,
            store,
        )

        stores_cluster = (
            processed_data_by_store_category.cluster_lookup_from_store_phys[store]
        )

        processed_data_by_store_category.elasticity_curves = (
            filter_and_assert_not_empty(
                processed_data_by_store_category.elasticity_curves,
                "elasticity df_cluster_filter",
                n.F_M_CLUSTER,
                stores_cluster,
            )
        )

        processed_data_by_store_category.pdf_item_counts = (
            processed_data.pdf_item_counts
        )
        processed_data_by_store_category.pdf_item_counts = filter_and_assert_not_empty(
            processed_data_by_store_category.pdf_item_counts,
            "pdf_item_counts",
            n.F_M_CLUSTER,
            stores_cluster,
        )

        # assign product info
        processed_data_by_store_category.product_info = filter_and_assert_not_empty(
            processed_data_by_store_category.product_info,
            "product info",
            n.F_STORE_PHYS_NO,
            store,
        )

        # assign store category dims
        processed_data_by_store_category.store_category_dims = (
            filter_and_assert_not_empty(
                processed_data_by_store_category.store_category_dims,
                "store_category_dims",
                n.F_STORE_PHYS_NO,
                store,
            )
        )

        processed_data_by_store_category.pdf_sales_penetration = (
            processed_data.pdf_sales_penetration
        )
        # keep and not assert could be empty
        processed_data_by_store_category.pdf_sales_penetration = (
            processed_data_by_store_category.pdf_sales_penetration.loc[
                processed_data_by_store_category.pdf_sales_penetration[n.F_M_CLUSTER]
                == stores_cluster
            ]
        )

        processed_data_by_store_category.pdf_margin_penetration = (
            processed_data.pdf_margin_penetration
        )
        # keep and not assert could be empty
        processed_data_by_store_category.pdf_margin_penetration = (
            processed_data_by_store_category.pdf_margin_penetration.loc[
                processed_data_by_store_category.pdf_margin_penetration[n.F_M_CLUSTER]
                == stores_cluster
            ]
        )

        processed_data_by_store_category.pdf_bay_data = processed_data.pdf_bay_data

        processed_data_by_store_category.supplier_own_brands_mapping = (
            processed_data.supplier_own_brands_mapping
        )
        processed_data_by_store_category.supplier_own_brands_request = (
            processed_data.supplier_own_brands_request
        )
        processed_data_by_store_category.supplier_item_mapping = (
            processed_data.supplier_item_mapping
        )

        scope = DataObjectScopeSpecific(
            region=region,
            banner=banner,
            dept=dept,
            store=store,
        )

        # filename = f"{proc_opt_location}processed_data_{reg_ban_dept_str}-{store_str}.pkl"

        DataObject = (
            RawDataRegionBannerDeptStoreRerun if rerun else RawDataRegionBannerDeptStore
        )

        data_raw_region_banner_dept_store = DataObject(
            cluster_lookup_from_store_phys=processed_data_by_store_category.cluster_lookup_from_store_phys,
            merged_clusters_df=processed_data_by_store_category.merged_clusters_df,
            elasticity_curves=processed_data_by_store_category.elasticity_curves,
            product_info=processed_data_by_store_category.product_info,
            location_df=processed_data_by_store_category.location_df,
            product_info_original=processed_data_by_store_category.product_info_original,
            shelve_space_df_original=processed_data_by_store_category.shelve_space_df_original,
            store_category_dims=processed_data_by_store_category.store_category_dims,
            shelve_space_df=processed_data_by_store_category.shelve_space_df,
            pdf_bay_data=processed_data_by_store_category.pdf_bay_data,
            pdf_item_counts=processed_data_by_store_category.pdf_item_counts,
            supplier_own_brands_mapping=processed_data_by_store_category.supplier_own_brands_mapping,
            supplier_own_brands_request=processed_data_by_store_category.supplier_own_brands_request,
            supplier_item_mapping=processed_data_by_store_category.supplier_item_mapping,
            pdf_sales_penetration=processed_data_by_store_category.pdf_sales_penetration,
            pdf_margin_penetration=processed_data_by_store_category.pdf_margin_penetration,
            scope=scope,
        )

        data_raw_region_banner_dept_store.save(path_opt_data_containers)

        # 1-row pandas DF
        pdf_result = pd.DataFrame(
            {
                "REGION": [region],
                "BANNER": [banner],
                "DEPT": [dept],
                "STORE": [store],
                "PATH_PRE_PROC_RAW_DATA_STORE": [
                    data_raw_region_banner_dept_store.path
                ],
            }
        )

        return pdf_result

    @F.pandas_udf(schema_out, F.PandasUDFType.GROUPED_MAP)
    def udf(pdf: pd.DataFrame):

        assert len(pdf) == 1, "pdf passed to udf must be 1 row"
        scope = pdf.to_dict()
        log.info(f"Start running for: {scope}")

        try:
            return logic_to_wrap(pdf)
        except Exception as ex:
            tb = traceback.format_exc()

            msg = f"""
            The task broke for scope: {scope} 
            execution threw an error:\n{tb}
            \nFailed to run task for scope: {scope} (see above).
            """

            log.info(msg)

            raise Exception(msg)

    return udf


def generate_and_run_udf_region_banner_dept_step_one(
    df_skeleton: SparkDataFrame,
    path_raw_opt_location: str,
    rerun: bool,
) -> SparkDataFrame:
    """
    Generates and runs the UDF function that is used to produce output of the
    task_opt_pre_proc_region_banner_dept_step_two task which is part of the
    pre-processing step for optimization.

    Parameters
    ----------
    data_raw_general : RawDataGeneral
        data container containing the required input data, see
        spaceprod.src.optimization.data_objects.RawDataGeneral

    pdf_list_of_reruns : pd.DataFrame
        input dataset generated in 'determine_margin_space_to_optimize'

    path_raw_opt_location :
        generic path within the run folder that contains paths

    rerun :
        whether or not to run in a 'rerun' mode for margin

    Returns
    -------
    skeleton dataset containing the dimension columns and the corresponding
    path to UDF output for each dimension combination
    """

    udf: Callable = generate_udf_pre_proc_region_banner_dept_step_one(
        path_opt_data_containers=path_raw_opt_location,
        rerun=rerun,
    )

    cols = [
        "REGION",
        "BANNER",
        "DEPT",
    ]

    # calling the udf (standard pyspark udf logic)
    df_skeleton = df_skeleton.repartition(*cols)
    df_result = df_skeleton.groupBy(*cols).apply(udf)
    df_result = backup_on_blob(spark, df_result)

    return df_result


def generate_and_run_udf_region_banner_dept_step_two(
    df_skeleton: SparkDataFrame,
    path_opt_data_containers: str,
    dependent_var: str,
    max_facings: int,
    use_macro_section_length: bool,
    enable_margin_reruns: bool,
    rerun: bool,
    hold_items_constant_from_file: bool,
    section_master_excluded: List[str],
    section_length_lower_deviation: float,
    section_length_upper_deviation: float,
    difference_of_facings_to_allocate_from_max_in_POG: int,
    legal_section_break_increments: Dict[str, int],
    legal_section_break_dict,
    minimum_shelves_assumption: int,
    possible_number_of_shelves_min: int,
    possible_number_of_shelves_max: int,
    overwrite_at_min_shelve_increment: bool,
    enable_localized_space: bool,
    overwrite_local_space_percentage: float,
    filter_for_test_negotiations: bool,
    run_on_theoretic_space: bool,
    facing_percentile: float,
) -> SparkDataFrame:
    """
    Generates and runs the UDF function that is used to produce output of the
    task_opt_pre_proc_region_banner_dept_step_two task which is part of the
    pre-processing step for optimization.

    Parameters
    ----------
    dependent_var : str
        sales or margin

    max_facings : int
        TODO

    use_macro_section_length : bool
        if this is set to true we use the macro allocated space per
        section in the micro opt instead of the current POG section length
        equivalent space

    enable_margin_reruns : bool
        if margin reruns should be enabled we save sales space for margin
        categories and use that as constraint to run those sections on margin
        again

    item_numbers_for_constant_treatment : bool

    section_master_excluded : List[str]
        TODO

    opt_config : Dict[str, Any]
        optimization configuration object

    path_opt_data_containers: str
        generic path within the run folder that contains paths

    rerun : bool
        whether or not to run in a 'rerun' mode for margin

    Returns
    -------
    the skeleton dataset with the output paths to resulting outputs of each
    UDF run and the corresponding store list as new columns
    """

    udf = generate_udf_pre_proc_region_banner_dept_step_two(
        path_opt_data_containers=path_opt_data_containers,
        dependent_var=dependent_var,
        max_facings=max_facings,
        use_macro_section_length=use_macro_section_length,
        enable_margin_reruns=enable_margin_reruns,
        rerun=rerun,
        hold_items_constant_from_file=hold_items_constant_from_file,
        section_master_excluded=section_master_excluded,
        section_length_lower_deviation=section_length_lower_deviation,
        section_length_upper_deviation=section_length_upper_deviation,
        difference_of_facings_to_allocate_from_max_in_POG=difference_of_facings_to_allocate_from_max_in_POG,
        legal_section_break_increments=legal_section_break_increments,
        legal_section_break_dict=legal_section_break_dict,
        minimum_shelves_assumption=minimum_shelves_assumption,
        possible_number_of_shelves_min=possible_number_of_shelves_min,
        possible_number_of_shelves_max=possible_number_of_shelves_max,
        overwrite_at_min_shelve_increment=overwrite_at_min_shelve_increment,
        enable_localized_space=enable_localized_space,
        overwrite_local_space_percentage=overwrite_local_space_percentage,
        filter_for_test_negotiations=filter_for_test_negotiations,
        run_on_theoretic_space=run_on_theoretic_space,
        facing_percentile=facing_percentile,
    )

    cols = [
        "REGION",
        "BANNER",
        "DEPT",
    ]

    df_skeleton = df_skeleton.repartition(*cols)
    df_result = df_skeleton.groupBy(*cols).apply(udf)
    df_result = backup_on_blob(spark, df_result)

    return df_result


def generate_and_run_udf_region_banner_dept_store(
    df_skeleton: SparkDataFrame,
    path_opt_data_containers: str,
    rerun: bool,
) -> SparkDataFrame:
    """
    Generates and runs the UDF function that is used to produce output of the
    task_opt_pre_proc_region_banner_dept_store task which is part of the
    preprocessing step for optimization.

    Parameters
    ----------

    df_skeleton: SparkDataFrame
        the "skeleton" dataset containing only required dimensions:
        customer / basket / item level / POG section / region / banner

    path_opt_data_containers: str
        generic path within the run folder that contains paths

    rerun : bool
        whether or not to run in a 'rerun' mode for margin

    Returns
    -------
    the skeleton dataset with the output paths to resulting outputs of each
    UDF run as a new column.
    """

    udf = generate_udf_pre_proc_region_banner_dept_store(
        path_opt_data_containers=path_opt_data_containers,
        rerun=rerun,
    )

    cols = [
        "REGION",
        "BANNER",
        "DEPT",
        "STORE",
    ]

    df_skeleton = df_skeleton.repartition(*cols)
    df_result = df_skeleton.groupBy(*cols).apply(udf)

    # same length as df_skeleton but with container paths column
    df_result = backup_on_blob(spark, df_result)

    return df_result
