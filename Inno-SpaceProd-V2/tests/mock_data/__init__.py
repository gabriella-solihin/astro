class MockData:
    """
    wrapper around mock data
     - provides convenient single-point access to all mock data
     - ensures that mock data is only imported here as it is being create
       on import, and we don't want to do that
    """

    @property
    def contactability_flag(self):
        from tests.mock_data.cust_flags import DF_CUST_FLAGS

        return DF_CUST_FLAGS

    @property
    def product(self):
        from tests.mock_data.product import DF_PRODUCT

        return DF_PRODUCT

    @property
    def location(self):
        from tests.mock_data.location_small import DF_LOCATION_SMALL

        return DF_LOCATION_SMALL

    @property
    def txnitem(self):
        from tests.mock_data.txnitem import DF_TXNITEM

        return DF_TXNITEM

    @property
    def competitors_big(self):
        from tests.mock_data.competitor import DF_COMPETITOR

        return DF_COMPETITOR

    @property
    def competitors(self):
        from tests.mock_data.competitor_small import DF_COMPETITOR_SMALL

        return DF_COMPETITOR_SMALL

    @property
    def demographics_5mins_drive(self):
        from tests.mock_data.demographics_small import DF_DEMOGRAPHICS_DATA_5_MIN_SMALL

        return DF_DEMOGRAPHICS_DATA_5_MIN_SMALL

    @property
    def demographics_10mins_drive(self):
        from tests.mock_data.demographics_small import DF_DEMOGRAPHICS_DATA_10_MIN_SMALL

        return DF_DEMOGRAPHICS_DATA_10_MIN_SMALL

    @property
    def demographics_15mins_drive(self):
        from tests.mock_data.demographics_small import DF_DEMOGRAPHICS_DATA_15_MIN_SMALL

        return DF_DEMOGRAPHICS_DATA_15_MIN_SMALL

    @property
    def store_list(self):
        from tests.mock_data.store_list_small import DF_STORE_LIST_SMALL

        return DF_STORE_LIST_SMALL

    @property
    def mapping(self):
        from tests.mock_data.mapping import DF_MAPPING

        return DF_MAPPING

    @property
    def apollo(self):
        from tests.mock_data.apollo_small import DF_APOLLO_SMALL

        return DF_APOLLO_SMALL

    @property
    def margin(self):
        from tests.mock_data.margin_small import DF_MARGIN_SMALL

        return DF_MARGIN_SMALL

    @property
    def calendar(self):
        from tests.mock_data.calendar import DF_CALENDAR

        return DF_CALENDAR

    @property
    def spaceman(self):
        from tests.mock_data.spaceman import DF_SPACEMAN

        return DF_SPACEMAN

    @property
    def feature_dashboard_names(self):
        from tests.mock_data.feature_dashboard_names import DF_FEATURE_DASHBOARD_NAMES

        return DF_FEATURE_DASHBOARD_NAMES

    @property
    def competitor_dashboard_names(self):
        from tests.mock_data.competitor_dashboard_names import (
            DF_COMPETITOR_DASHBOARD_NAMES,
        )

        return DF_COMPETITOR_DASHBOARD_NAMES

    @property
    def section_master_override(self):
        from tests.mock_data.section_master_override import DF_SECTION_MASTER_OVERRIDE

        return DF_SECTION_MASTER_OVERRIDE

    @property
    def pog_department_mapping_file(self):
        from tests.mock_data.pog_department_mapping_file import (
            DF_POG_DEPARTMENT_MAPPING_FILE,
        )

        return DF_POG_DEPARTMENT_MAPPING_FILE

    @property
    def region_banner_clusters_for_margin_reruns(self):
        from tests.mock_data.region_banner_clusters_for_margin_reruns import (
            DF_REGION_BANNER_CLUSTERS_FOR_MARGIN_RERUNS,
        )

        return DF_REGION_BANNER_CLUSTERS_FOR_MARGIN_RERUNS

    @property
    def region_banner_sections_for_margin_reruns(self):
        from tests.mock_data.region_banner_sections_for_margin_reruns import (
            DF_REGION_BANNER_SECTIONS_FOR_MARGIN_RERUNS,
        )

        return DF_REGION_BANNER_SECTIONS_FOR_MARGIN_RERUNS

    @property
    def summary_per_store_cat_sales(self):
        from tests.mock_data.summary_per_store_cat_sales import (
            DF_SUMMARY_PER_STORE_CAT_SALES,
        )

        return DF_SUMMARY_PER_STORE_CAT_SALES

    @property
    def merged_clusters_external(self):
        from tests.mock_data.merged_clusters_revised_small import (
            DF_MERGED_CLUSTERS_EXTERNAL_SMALL,
        )

        return DF_MERGED_CLUSTERS_EXTERNAL_SMALL

    @property
    def pog_deviations(self):
        from tests.mock_data.pog_deviation import (
            DF_POG_DEVIATION,
        )

        return DF_POG_DEVIATION

    @property
    def supplier_item_mapping_path(self):
        from tests.mock_data.supplier_item_mapping import DF_SUPPLIER_ITEM_MAPPING

        return DF_SUPPLIER_ITEM_MAPPING

    @property
    def own_brands_path(self):
        from tests.mock_data.own_brands import DF_OWN_BRANDS

        return DF_OWN_BRANDS

    @property
    def localized_space_request(self):
        from tests.mock_data.localized_space_request import DF_LOCALIZED_SPACE_REQUEST

        return DF_LOCALIZED_SPACE_REQUEST

    @property
    def localized_default_space(self):
        from tests.mock_data.localized_default_space import DF_LOCALIZED_DEFAULT_SPACE

        return DF_LOCALIZED_DEFAULT_SPACE

    @property
    def supp_own_brands_request(self):
        from tests.mock_data.supp_own_brands_request import DF_SUPP_OWN_BRANDS_REQUEST

        return DF_SUPP_OWN_BRANDS_REQUEST

    @property
    def french_to_english_translations(self):
        from tests.mock_data.french_to_english_section_master_translations import (
            DF_FRENCH_TO_ENGLISH_TRANSLATIONS,
        )

        return DF_FRENCH_TO_ENGLISH_TRANSLATIONS

    #########################################################
    # GENERATED DATASETS
    #########################################################

    @property
    def combined_pog_processed(self):
        from tests.mock_data.combined_pog_processed import DF_COMBINED_POG_PROCESSED

        return DF_COMBINED_POG_PROCESSED

    @property
    def final_need_states(self):
        from tests.mock_data.final_need_states import DF_FINAL_NEED_STATES

        return DF_FINAL_NEED_STATES

    @property
    def merged_clusters(self):
        from tests.mock_data.merged_clusters import DF_MERGED_CLUSTERS

        return DF_MERGED_CLUSTERS

    @property
    def final_clustering_data(self):
        from tests.mock_data.final_clustering_data import DF_FINAL_CLUSTERING_DATA

        return DF_FINAL_CLUSTERING_DATA

    @property
    def clustering_output_assignment(self):
        from tests.mock_data.clustering_output_assignment import (
            DF_CLUSTERING_OUTPUT_ASSIGNMENT,
        )

        return DF_CLUSTERING_OUTPUT_ASSIGNMENT

    @property
    def combined_sales_levels(self):
        from tests.mock_data.combined_sales_levels import DF_COMBINED_SALES_LEVELS

        return DF_COMBINED_SALES_LEVELS

    @property
    def margin_penetration_proportions(self):
        from tests.mock_data.margin_penetration_proportions import (
            DF_MARGIN_PENETRATION_PROPORTIONS,
        )

        return DF_MARGIN_PENETRATION_PROPORTIONS

    @property
    def sales_penetration_proportions(self):
        from tests.mock_data.sales_penetration_proportions import (
            DF_SALES_PENETRATION_PROPORTIONS,
        )

        return DF_SALES_PENETRATION_PROPORTIONS

    @property
    def items_held_constant(self):
        from tests.mock_data.items_held_constant import DF_ITEMS_HELD_CONSTANT

        return DF_ITEMS_HELD_CONSTANT

    @property
    def prod2vec_raw(self):
        from tests.mock_data.prod2vec_raw import DF_PROD2VEC_RAW

        return DF_PROD2VEC_RAW


integration_test_mock_data = MockData()
