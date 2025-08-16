"""
This module contains definition of column names as python objects
By using this column name "glossary" we simplify the life of a developer
since column names are specified once here, and get reused across the codebase
as python objects which allows for auto-completion by IDE and avoids
misspelling
"""


class ColumnNames:

    # Column names of input data
    F_STORE_NO = "Store_No"  # First three characters of the subtype -> str
    F_STORE_PHYS_NO = "Store_Physical_Location_No"
    F_BANNER = "Banner"
    F_REGION_DESC = "Region_Desc"
    F_DEPARTMENT = "Department"
    F_STORE_NAME = "Store_Name"
    F_M_CLUSTER = "M_Cluster"  # merged cluster
    F_ITEM_COUNT = "Item_Count"

    ###### GENERAL COLUMNS
    F_ITEM_NO = "Item_No"
    F_ITEM_NAME = "Item_Name"
    F_UNIQUE_ITEMS = "Unique_Items"
    F_UNIQUE_CUR_ASSORT = "Unique_Items_Cur_Assorted"
    F_UNIQUE_OPT_ASSORT = "Unique_Items_Opt_Assorted"
    F_OPT_MINUS_CUR_FACINGS = "Opt_Minus_Cur_Facings"
    F_AVG_OPT_MINUS_CUR_FACINGS = "Avg_Opt_Minus_Cur_Facings"
    F_CUR_OPT_FACING_SAME = "Cur_Opt_Same_Facing"
    F_PERC_ITEMS_W_FACING_DELTA = "Perc_Items_W_Facing_Changes"

    ###### LOCATION COLUMNS
    F_RETAIL_LOC_SK = "Retail_Outlet_Location_Sk"
    F_BANNER_KEY = "Banner_Key"
    #

    ###### PRODUCT HIERARCHY COLUMNS
    F_LVL5_NAME = "Lvl5_Name"
    F_LVL4_NAME = "Lvl4_Name"
    F_LVL3_NAME = "Lvl3_Name"
    F_LVL2_NAME = "Lvl2_Name"
    F_CANNIB_ID = "Cannib_Id"
    F_CANNIB_ID_IDX = "Cannib_Id_Idx"
    F_NEED_STATE = "Need_State"
    F_NEED_STATE_IDX = "Need_State_Idx"
    F_CLUSTER_NEED_STATE = "Cluster_Need_State"
    F_CLUSTER_NEED_STATE_IDX = "Cluster_Need_State_Idx"
    F_SALES = "Sales"
    F_CUR_SALES = "Cur_Sales"
    F_CUR_MARGIN = "Cur_Margin"
    F_CUR = "Cur_"
    F_OPT = "Opt_"
    F_OPT_SALES = "Opt_Sales"
    F_OPT_MARGIN = "Opt_Margin"
    F_MARGIN = "Margin"

    ###DIMENSIONS
    F_WIDTH_IN = "Width"
    # F_CUR_WIDTH_X_FAC = "Cur_Width_x_Facing"
    # F_OPT_WIDTH_X_FAC = "Opt_Width_x_Facing"
    F_CUR_WIDTH_X_FAC = "Cur_Width_X_Facing"
    F_OPT_WIDTH_X_FAC = "Opt_Width_X_Facing"
    F_CUR_SALES_PER_FT = "Cur_sales_per_foot"
    F_OPT_SALES_PER_FT = "Opt_sales_per_foot"
    F_CUR_AVG_SALES_PER_FACING = "Cur_avg_sales_per_facing"
    F_OPT_AVG_SALES_PER_FACING = "Opt_avg_sales_per_facing"
    F_FACINGS = "Facings"
    F_FOOTAGE = "Footage"
    F_SECTION_NAME = "Section_Name"
    F_SECTION_NAME_OLD = "Section Name"
    F_LINEAR_SPACE_PER_LEGAL_SECTION_BREAK = "Lin_Space_Per_Break"
    F_SECTION_LENGTH_IN = "Length_Section_Inches"
    F_LEGAL_BREAKS_IN_SECTION = "Legal_Breaks_In_Section"
    F_SECTION_LENGTH_IN_OPT = "Length_Section_Optimizable"
    F_LEGAL_INCR_WIDTH = "Legal_Increment_Width"
    F_NO_OF_SHELVES = "No_Of_Shelves"
    F_THEORETIC_LIN_SPACE = "Theoretic_lin_space"
    F_CUR_SECTION_WIDTH_DEV = "Cur_Section_Width_Deviation"

    F_SECTION_MASTER = "Section_Master"
    F_SECTION_MASTER_OLD = "Section Master"
    F_SECTION_MASTER_IDX = "Section_Master_Idx"

    F_CUR_FACINGS = "Current_Facings"
    F_OPT_FACINGS = "Optim_Facings"
    F_CONST_TREAT = "Constant_Treatment"

    # MACRO related
    F_CUR_SECTION_LEN = "Current_Section_Length"
    F_OPT_SECTION_LEN = "Optim_Section_Length"

    # Dimension calculations
    F_MAX_FACINGS_IN_OPT = "max_facings_in_opt"

    F_LVL5_ID = "Lvl5_Id"
    F_CATEGORY_ID = "Category_Id"
    F_LVL3_ID = "Lvl3_Id"
    F_LVL2_ID = "Lvl2_Id"

    ### Constraints
    F_SUPPLIER_ID = "Supplier_Id"
    F_SUPPLIER_NM = "Supplier_Nm"
    F_OWN_BRANDS_FLAG = "Own_Brands_Flag"
    F_OWN_BRAND_NM = "Own_Brands_Nm"
    F_OWN_BRANDS_CD = "Own_Brands_Cd"
    F_PERCENTAGE_SPACE = "Percentage_Space"
    F_MAPPING_EXISTS = "Mapping_Exists"
    F_SUPPLIER_OB_ID = "Supplier_Ob_Id"
    F_SPACE_TO_ADD = "Space_To_Add"
    # Local item reserve space in inches
    F_LOCAL_ITEM_WIDTH = "Local_Item_Width"

    """    
    F_SHELVE_SPACE_DF_NEW_COL_NAMES = {
        "Store": F_STORE_NO,
        "Stockcode": F_ITEM_NO,
        "Store Name": F_STORE_NAME,
        "Store_Name": F_STORE_NAME,
        "Length_Section": F_SECTION_LENGTH_IN,
        "Section_Name": F_SECTION_NAME,
        "Length_Section_Footage": F_FOOTAGE,
    }
    """

    F_SHELVE_SPACE_DF_NEW_COL_NAMES = {
        "STORE_NO": F_STORE_NO,
        "ITEM_NO": F_ITEM_NO,
        "SECTION_NAME": F_SECTION_NAME,
        "SECTION_MASTER": F_SECTION_MASTER,
        "WIDTH": F_WIDTH_IN,
        "FACINGS": F_FACINGS,
    }

    F_POG_DEPT_MAPPING_DF_NEW_COL_NAMES = {
        "REGION": F_REGION_DESC,
        "BANNER": F_BANNER,
        "SECTION_MASTER": F_SECTION_MASTER,
        "DEPARTMENT": F_DEPARTMENT,
    }

    F_MERGED_CLUSTERS_NEW_NAMES = {
        "Banner_Key": F_BANNER_KEY,
        "Store_Physical_Location_No": F_STORE_PHYS_NO,
        "Ban_Name": F_STORE_NAME,
        "Ban_No": F_STORE_NO,
        "Banner": F_BANNER,
        "Merge_Cluster": F_M_CLUSTER,
        "MERGE_CLUSTER": F_M_CLUSTER,
        "merge_cluster": F_M_CLUSTER,
        "Merged_Cluster": F_M_CLUSTER,
        "STORE_PHYSICAL_LOCATION_NO": F_STORE_PHYS_NO,
    }

    F_MERGED_CLUSTER_NEW_COLS = [F_STORE_PHYS_NO, F_STORE_NO, F_M_CLUSTER]

    F_ELASTICITY_CURVE_NEW_NAMES = {
        "Region_Desc": F_REGION_DESC,
        "Banner": F_BANNER,
        "Item_No": F_ITEM_NO,
        "Item_Name": F_ITEM_NAME,
        "Lvl4_Name": F_LVL4_NAME,
        "Need_State": F_NEED_STATE,
        "Need_State_Idx": F_NEED_STATE_IDX,
        "Section_Master_Idx": F_SECTION_MASTER_IDX,
        "Store_Physical_Location_No": F_STORE_PHYS_NO,
    }

    #### RESULTS PROCESSING COLUMNS LIST
    F_SHELVE_NEW_COLS = [
        F_STORE_PHYS_NO,
        F_ITEM_NO,
        F_SECTION_MASTER,
        F_FACINGS,
        F_WIDTH_IN,
    ]

    F_X_PREP_DF_NEW_COLS = [
        F_STORE_PHYS_NO,
        F_ITEM_NO,
        F_FACINGS,
        F_CONST_TREAT,
    ]

    F_ELASTICITY_SALES_NEW_COLS = [
        F_M_CLUSTER,
        F_ITEM_NO,
        F_FACINGS,
        F_NEED_STATE,
        F_NEED_STATE_IDX,
        F_SALES,
    ]

    F_ELASTICITY_MARGIN_NEW_COLS = [
        F_M_CLUSTER,
        F_ITEM_NO,
        F_FACINGS,
        F_NEED_STATE,
        F_NEED_STATE_IDX,
        F_MARGIN,
    ]

    F_SUPPLIER_OWN_BRANDS_ANALYSIS_COLS = [
        F_M_CLUSTER,
        F_STORE_PHYS_NO,
        F_SECTION_MASTER,
        F_SUPPLIER_ID,
        F_OWN_BRANDS_FLAG,
        F_SUPPLIER_OB_ID,
        F_CUR_WIDTH_X_FAC,
        F_CUR_WIDTH_X_FAC + "_Total",
        F_OPT_WIDTH_X_FAC,
        F_OPT_WIDTH_X_FAC + "_Total",
        "Opt_Percent_Space",
        "Requested_Percent_Space",
        F_MAPPING_EXISTS,
    ]

    F_FINAL_SUPPLIER_SPACE_ANALYSIS_COLS = [
        F_REGION_DESC,
        F_BANNER,
        F_DEPARTMENT,
        F_M_CLUSTER,
        F_STORE_PHYS_NO,
        F_SECTION_MASTER,
        F_SUPPLIER_ID,
        F_OWN_BRANDS_FLAG,
        F_SUPPLIER_OB_ID,
        F_CUR_WIDTH_X_FAC,
        F_CUR_WIDTH_X_FAC + "_Total",
        F_OPT_WIDTH_X_FAC,
        F_OPT_WIDTH_X_FAC + "_Total",
        "Opt_Percent_Space",
        "Post_Unit_Prop_Opt_Percent_Space",
        "Post_Unit_" + F_OPT_WIDTH_X_FAC,
        "Post_Unit_" + F_OPT_WIDTH_X_FAC + "_Total",
        "Final_" + F_OPT_WIDTH_X_FAC,
        "Final_" + F_OPT_WIDTH_X_FAC + "_Total",
        "Final_Opt_Percent_Space",
        "Requested_Percent_Space",
        F_MAPPING_EXISTS,
    ]

    F_SUP_REQUEST_NEW_COLS = [
        F_REGION_DESC,
        F_BANNER,
        F_SECTION_MASTER,
        F_SUPPLIER_ID,
        F_OWN_BRANDS_FLAG,
        F_PERCENTAGE_SPACE,
    ]

    ### RESULTS MACRO COLUMNS LISTS

    F_SECTION_NEW_COLS = [
        F_REGION_DESC,
        F_STORE_PHYS_NO,
        F_SECTION_MASTER,
        F_SECTION_LENGTH_IN,
    ]

    F_X_PREP_MACRO_DF_NEW_COLS = [
        F_REGION_DESC,
        F_STORE_PHYS_NO,
        F_SECTION_MASTER,
        F_SECTION_LENGTH_IN,
        F_CONST_TREAT,
    ]

    F_MACRO_ELASTICITY_SALES_NEW_COLS = [
        F_M_CLUSTER,
        F_REGION_DESC,
        F_SECTION_MASTER,
        F_SECTION_MASTER_IDX,
        F_SECTION_LENGTH_IN,
        F_SALES,
    ]

    F_MACRO_ELASTICITY_MARGIN_NEW_COLS = [
        F_M_CLUSTER,
        F_REGION_DESC,
        F_SECTION_MASTER,
        F_SECTION_MASTER_IDX,
        F_SECTION_LENGTH_IN,
        F_MARGIN,
    ]

    F_SECTION_RESULTS_COL_FOR_MARGIN_RERUN = [
        F_M_CLUSTER,
        F_REGION_DESC,
        F_SECTION_MASTER,
        F_BANNER,
        F_OPT_WIDTH_X_FAC,
        F_STORE_PHYS_NO,
        F_DEPARTMENT,
    ]

    # transaction cols
    F_TRX_RAW_COL_LIST = [
        "REGION_DESC",
        "RETAIL_OUTLET_LOCATION_SK",
        "region",
        "Banner",
        "ITEM_SK",
        "ITEM_NO",
        "TRANSACTION_RK",
        "ITEM_QTY",
        "ITEM_WEIGHT",
        "SELLING_RETAIL_AMT",
        "CALENDAR_DT",
        "year",
        "month",
        "LVL5_NAME",
        "LVL4_NAME",
        "ITEM_NAME",
        "CATEGORY_ID",
        "STORE_PHYSICAL_LOCATION_NO",
        "STORE_NO",
    ]
    ###elasticity curves

    F_SECTION_MASTER = "Section_Master"
    F_SECTION_MASTER_IDX = "Section_Master_Idx"

    F_BAY_DATA_MACRO_NEW_NAMES = {
        "REGION_DESC": F_REGION_DESC,
        "STORE_PHYSICAL_LOCATION_NO": F_STORE_PHYS_NO,
        "SECTION_MASTER": F_SECTION_MASTER,
        "LENGTH_SECTION_INCHES": F_SECTION_LENGTH_IN,
        "SALES": F_SALES,
        "E2E_MARGIN_TOTAL": F_MARGIN,
    }

    F_BAY_DATA_MICRO_NEW_NAMES = {
        "REGION_DESC": F_REGION_DESC,
        "STORE_PHYSICAL_LOCATION_NO": F_STORE_PHYS_NO,
        "ITEM_NO": F_ITEM_NO,
        "ITEM_NAME": F_ITEM_NAME,
        "LVL4_NAME": F_LVL4_NAME,
        "CANNIB_ID": F_CANNIB_ID,
        "NEED_STATE": F_NEED_STATE,
        "SALES": F_SALES,
        "E2E_MARGIN_TOTAL": F_MARGIN,
        "FACINGS": F_FACINGS,
    }

    # opt sales proportion adjsutement columns
    F_SALES_PROPORTION_FINAL_COLS = [
        F_M_CLUSTER,
        F_STORE_PHYS_NO,
        F_SECTION_MASTER,
        F_NEED_STATE_IDX,
        F_NEED_STATE,
        F_ITEM_NO,
        F_ITEM_NAME,
        F_LVL4_NAME,
        F_WIDTH_IN,
        F_CONST_TREAT,
        F_CUR_FACINGS,
        F_CUR_SALES,
        F_OPT_SALES,
        F_OPT_FACINGS,
    ]

    F_MARGIN_PROPORTION_FINAL_COLS = [
        F_M_CLUSTER,
        F_STORE_PHYS_NO,
        F_SECTION_MASTER,
        F_NEED_STATE_IDX,
        F_NEED_STATE,
        F_ITEM_NO,
        F_ITEM_NAME,
        F_LVL4_NAME,
        F_WIDTH_IN,
        F_CONST_TREAT,
        F_CUR_FACINGS,
        F_CUR_MARGIN,
        F_OPT_MARGIN,
        F_OPT_FACINGS,
    ]

    def get(self, col_name):
        """
        for compatibility with existing code
        sometimes columns are accessed as ColumnNames().get("F_SOME_COL")
        """
        return getattr(self, col_name)


# for compatibility with existing code
def get_col_names():
    return ColumnNames()


n = get_col_names()

# Cannibalization Group Names
cannib_id = "CANNIB_ID"
lv5_id = "LVL5_ID"
ah_lv5_id = "AH_LVL5_ID"

# Item Hierarchy Column Names
item_no = "ITEM_NO"
item_sk = "ITEM_SK"
item_name = "ITEM_NAME"
item_size_cd = "ITEM_SIZE_CD"
item_unit_cd = "ITEM_UNIT_CD"
item_uom_cd = "ITEM_UOM_CD"
brand_eng_nm = "BRAND_ENG_NM"

# Store Column Names
active_status_cd = "ACTIVE_STATUS_CD"
retail_outlet_location_sk = "RETAIL_OUTLET_LOCATION_SK"
national_banner_cd = "NATIONAL_BANNER_CD"

# Transaction Column Names
calendar_date = "CALENDAR_DT"
customer_sk = "CUSTOMER_SK"
transaction_rk = "TRANSACTION_RK"


# Canni Column Names
region_desc = "REGION_DESC"
prod_pairs = "PROD_PAIRS"
item_a = "ITEM_A"
item_b = "ITEM_B"
chi_score = "CHI_SCORE"
chi_score_min = "chi_score_min"
item_family = "ITEM_FAMILY"
tot_cust_purch = "TOTAL_CUSTS_PURCH"
