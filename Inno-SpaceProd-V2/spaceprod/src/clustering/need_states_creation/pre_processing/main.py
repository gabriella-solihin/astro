from inno_utils.loggers import log
from spaceprod.src.clustering.need_states_creation.data_contracts import (
    DATA_CONTRACT_APOLLO,
    DATA_CONTRACT_SPACEMAN,
)
from spaceprod.src.clustering.need_states_creation.helpers import validate_dataset
from spaceprod.src.clustering.need_states_creation.pre_processing.helpers import (
    ItemLostInNSReason,
    add_customer_card_id,
    aggregate_to_cust_basket_item,
    capture_items_lost,
    clean_section_master,
    clean_spaceman,
    combine_pogs_into_section_masters,
    dedup_apollo,
    dedup_spaceman,
    filter_apollo,
    get_consistent_item_widths,
    get_core_trans,
    get_distinct_region_banner_item_from_pairs,
    get_frequent_visitors,
    get_items_diff_trans,
    get_items_lost_after_explosion,
    get_section_master_modes,
    impute_spaceman_null_facings,
    log_entity_counts,
    log_item_count,
    merge_pogs,
    patch_pog,
    remove_low_volume_items,
    translate_spaceman,
    validate_using_exec_id,
    combine_items_lost,
    add_case_pack_info,
    add_product_info,
)
from spaceprod.utils.data_helpers import backup_on_blob
from spaceprod.utils.data_transformation import check_invalid
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context import context
from spaceprod.utils.space_context.spark import spark


@timeit
def task_pre_process_apollo_data():

    ###########################################################################
    # OBTAIN REQUIRED CONFIGS and CONFIG PARAMS
    ###########################################################################
    conf = context.config["clustering"]["need_states_config"]["pog_processing"]
    thresh_ratio_missing_store_pogs = conf["thresh_ratio_missing_store_pogs"]
    thresh_ratio_missing_pogs = conf["thresh_ratio_missing_pogs"]

    ###########################################################################
    # READ IN REQUIRED DATA
    ###########################################################################

    df_apollo = context.data.read("apollo")
    df_sm_override = context.data.read("section_master_override")
    df_location = context.data.read("location")

    ###########################################################################
    # VALIDATE INPUT DATASETS
    ###########################################################################

    df_apollo = validate_dataset(
        df=df_apollo,
        data_contract=DATA_CONTRACT_APOLLO,
    )

    ###########################################################################
    # CALL REQUIRED HELPER FUNCTIONS TO PROCESS DATA
    ###########################################################################

    # generate item<->pog lookup dataset
    df_apollo_cleaned = clean_section_master(
        df_apollo=df_apollo,
    )

    df_apollo_deduped = dedup_apollo(df_apollo=df_apollo_cleaned)

    df_apollo_filtered = filter_apollo(df_apollo=df_apollo_deduped)

    df_apollo_with_section_masters = patch_pog(
        df_pog=df_apollo_filtered,
        df_sm_override=df_sm_override,
        df_location=df_location,
        thresh_ratio_missing_store_pogs=thresh_ratio_missing_store_pogs,
        thresh_ratio_missing_pogs=thresh_ratio_missing_pogs,
    )

    ###########################################################################
    # WRITE OUTPUT DATA
    ###########################################################################

    context.data.write(dataset_id="apollo_processed", df=df_apollo_with_section_masters)

    ###########################################################################
    # VALIDATE OUTPUT DATA
    ###########################################################################

    # ensure there are no invalid SECTION_MASTER's
    check_invalid(context.data.read("apollo_processed"), ["SECTION_MASTER"])

    # check to ensure consistency with EXEC_ID (see docstring for details)
    validate_using_exec_id(df=context.data.read("apollo_processed"))


@timeit
def task_pre_process_spaceman_data():

    ###########################################################################
    # READ IN REQUIRED DATA
    ###########################################################################
    log.info("Running with impute")
    df_spaceman = context.data.read("spaceman")
    df_translations_sm = context.data.read("french_to_english_translations")

    ###########################################################################
    # VALIDATE CERTAIN INPUT DATASETS
    ###########################################################################

    df_spaceman = validate_dataset(
        df=df_spaceman,
        data_contract=DATA_CONTRACT_SPACEMAN,
    )

    ###########################################################################
    # CALL REQUIRED HELPER FUNCTIONS TO PROCESS DATA
    ###########################################################################

    # generate item<->pog lookup dataset
    df_spaceman_cleaned = clean_spaceman(df_spaceman=df_spaceman)

    df_spaceman_deduped = dedup_spaceman(df_spaceman=df_spaceman_cleaned)

    df_spaceman_english = translate_spaceman(
        df_spaceman=df_spaceman_deduped,
        df_translations_sm=df_translations_sm,
    )

    df_spaceman_imputed = impute_spaceman_null_facings(df_spaceman=df_spaceman_english)

    ###########################################################################
    # SAVING RESULTS
    ###########################################################################

    context.data.write(dataset_id="spaceman_processed", df=df_spaceman_imputed)


@timeit
def task_concatenate_pog_data():
    """
    this task does 3 things:
    1. combines processed spaceman and apollo data
    2. does post processing on combined POG data to ensure consistency in SMs
    3. adds additional columns: case pack info, UOM, UPC
    """
    ###########################################################################
    # READ IN REQUIRED DATA
    ###########################################################################

    df_spaceman = context.data.read("spaceman_processed")
    df_apollo = context.data.read("apollo_processed")
    df_location = context.data.read("location")
    df_product = context.data.read("product")

    ###########################################################################
    # CALL REQUIRED HELPER FUNCTIONS TO PROCESS DATA
    ###########################################################################
    df_pog = merge_pogs(df_apollo=df_apollo, df_spaceman=df_spaceman)
    df_pog = get_consistent_item_widths(df_pog=df_pog)
    df_pog = get_section_master_modes(df_pog=df_pog, df_location=df_location)
    df_pog, df_many_to_one_pogs = combine_pogs_into_section_masters(df_pog=df_pog)

    # add additional information to POG data
    df_pog = add_case_pack_info(df_pog=df_pog, df_apollo=df_apollo)

    # add product info to the POG data
    df_pog = add_product_info(df_pog=df_pog, df_product=df_product)

    ###########################################################################
    # SAVING RESULTS
    ###########################################################################

    context.data.write(dataset_id="combined_pog_processed", df=df_pog)
    context.data.write(dataset_id="combined_sections", df=df_many_to_one_pogs)

    ###########################################################################
    # VALIDATION
    ###########################################################################

    check_invalid(context.data.read("combined_pog_processed"), ["SECTION_MASTER"])


@timeit
def task_preprocess_data():
    """This function preprocess all the need state creation data including
    transactions, POG sections, and date filtering
    """

    # determine which config(s) we need for this task
    config_ns = context.config["clustering"]["need_states_config"]
    config_scope = context.config["scope"]

    ###########################################################################
    # GET REQUIRED CONFIG SECTIONS
    ###########################################################################

    conf_input = config_ns["need_states_inputs"]

    ###########################################################################
    # ACCESS THE REQUIRED CONFIG PARAMETERS
    ###########################################################################

    banners = config_scope["banners"]
    regions = config_scope["regions"]
    visit_thresh = conf_input["visit_thresh"]
    item_vol_thresh = conf_input["item_vol_thresh"]
    st_date = config_scope["st_date"]
    end_date = config_scope["end_date"]
    pog_section_list = config_scope["pog_section_masters"]

    ###########################################################################
    # READ IN REQUIRED DATA
    ###########################################################################

    df_combined_pog_processed = context.data.read("combined_pog_processed")
    df_trans = context.data.read("txnitem", regions=regions)
    df_banner_lookup = context.data.read("location")
    df_prod_hierarchy = context.data.read("product")
    df_cust_reachable = context.data.read("contactability_flag")

    ###########################################################################
    # CALL REQUIRED HELPER FUNCTIONS TO PROCESS DATA
    ###########################################################################

    msg = f"""
    Running Need States creation pre-processing on the following scope:
     - st_date: {st_date}
     - end_date: {end_date}
     - pog_section_list: {pog_section_list or 'ALL'}
     - banners: {banners or 'ALL'}
     - regions: {regions or 'ALL'}
    """

    log.info(msg)

    # joining main input datasets together, i.e.
    # txnitem + product + location + POG, etc
    # the output of this this is the what we are starting with, next we
    # will be filtering on entities based on given thresholds
    df_trans_processed = get_core_trans(
        df_trans=df_trans,
        df_banner_lookup=df_banner_lookup,
        df_prod_hierarchy=df_prod_hierarchy,
        df_item_pog_section_lookup=df_combined_pog_processed,
        banner=banners,
        pog_section_list=pog_section_list,
        st_date=st_date,
        end_date=end_date,
    )

    df_trans_processed = backup_on_blob(spark, df_trans_processed)

    # Append the customer card ID
    df_trans_processed_with_cust = add_customer_card_id(
        df=df_trans_processed,
        df_cust_reachable=df_cust_reachable,
    ).persist()

    df_items_lost_only_valid_customer_card = capture_items_lost(
        df_start=df_trans_processed,
        df_end=df_trans_processed_with_cust,
        reason=ItemLostInNSReason.ONLY_VALID_CUSTOMER_CARD,
    )

    # Make data unique by customer, basket and item
    df_dims = aggregate_to_cust_basket_item(
        df=df_trans_processed_with_cust,
    )

    df_trans_processed_with_cust.unpersist()

    df_dims = backup_on_blob(spark, df_dims)

    # Keep only the customers visiting more than the specified threshold
    df_freq = get_frequent_visitors(
        df=df_dims,
        visit_thresh=visit_thresh,
    ).persist()

    df_items_lost_visit_thresh = capture_items_lost(
        df_start=df_dims,
        df_end=df_freq,
        reason=ItemLostInNSReason.VISIT_THRESH,
    )

    # Keep only items purchased by more customers than the specified threshold
    df_low_vol = remove_low_volume_items(
        df=df_freq,
        item_vol_thresh=item_vol_thresh,
    ).persist()

    df_items_lost_vol_thresh = capture_items_lost(
        df_start=df_freq,
        df_end=df_low_vol,
        reason=ItemLostInNSReason.VOL_THRESH,
    )

    df_freq.unpersist()

    # Keep only the items purchased within different transactions
    # creates pairs - turns ITEM_NO -> ITEM_A + ITEM+B
    df_cust_item_diff = get_items_diff_trans(
        df=df_low_vol,
    ).persist()

    df_cust_item_diff_set = get_distinct_region_banner_item_from_pairs(
        df=df_cust_item_diff
    )

    # get the list of items that were lost during the pairs explosion
    df_items_lost_pairs_creation = capture_items_lost(
        df_start=df_low_vol,
        df_end=df_cust_item_diff_set,
        reason=ItemLostInNSReason.PAIRS_CREATION,
    )

    # combine all slices of lost items into a single output for future use
    # all of these slices will be set to NS = -1
    df_items_lost = combine_items_lost(
        df_items_lost_only_valid_customer_card=df_items_lost_only_valid_customer_card,
        df_items_lost_visit_thresh=df_items_lost_visit_thresh,
        df_items_lost_vol_thresh=df_items_lost_vol_thresh,
        df_items_lost_pairs_creation=df_items_lost_pairs_creation,
    )

    context.data.write(
        dataset_id="pre_processing",
        df=df_cust_item_diff,
        partition_by=None,
        allow_empty=False,
        append=False,
    )

    context.data.write(
        dataset_id="items_lost_ns_pre_proc",
        df=df_items_lost,
        partition_by=None,
        allow_empty=True,
        append=False,
    )

    df_low_vol.unpersist()
    df_cust_item_diff.unpersist()

    log_entity_counts(df=context.data.read("pre_processing"))
