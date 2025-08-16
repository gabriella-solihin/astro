from spaceprod.src.adjacencies.association_distances.helpers import *
from spaceprod.utils.data_helpers import backup_on_blob
from spaceprod.utils.decorators import timeit
from spaceprod.utils.space_context.spark import spark
from spaceprod.utils.space_context import context


@timeit
def task_run_generate_association_distances():
    """
    task that calculates the association scores,
    which is used as distance values by the TSP solver for macro adjacency.
    """

    ###########################################################################
    # REQUIRED CONFIGS
    ###########################################################################
    conf_adj = context.config["adjacencies"]["adjacencies_config"]
    config_scope = context.config["scope"]

    ###########################################################################
    # REQUIRED CONFIG PARAMS
    ###########################################################################
    # List containing banner names to include
    banner = config_scope["banners"]
    # List containing regions to include
    regions = config_scope["regions"]
    # List containing departments to include
    department = config_scope["adjacencies_departments"]
    # Start date from which to pull transactions in the format YYYY-MM-DD
    st_date = config_scope["st_date"]
    # End date to pull transactions in the format YYYY-MM-DD
    end_date = config_scope["end_date"]
    # temporary list of section masters to include / patch with
    list_adj_scope = conf_adj["adjacencies_scope"]

    ###########################################################################
    # READ IN REQUIRED DATA
    ###########################################################################

    df_trans_raw = context.data.read("txnitem", regions=regions)
    df_location = context.data.read("location")
    df_prod_hierarchy = context.data.read("product")
    df_item_pog_section_lookup = context.data.read("combined_pog_processed")
    df_pog_section_dept_lookup = context.data.read("pog_department_mapping_file")

    ###########################################################################
    # CALL REQUIRED HELPER FUNCTIONS TO PROCESS DATA
    ###########################################################################

    msg = f"""
    Running macro adjacency on the following scope:
    - st_date: {st_date}
    - end_date: {end_date}
    - region: {regions}
    - banner: {banner}
    - department: {department}
    """

    log.info(msg)

    # Pull core transactions
    trans_df = get_core_trans(
        df_trans=df_trans_raw,
        df_location=df_location,
        df_prod_hierarchy=df_prod_hierarchy,
        df_item_pog_section_lookup=df_item_pog_section_lookup,
        df_pog_section_dept_lookup=df_pog_section_dept_lookup,
        region=regions,
        banner=banner,
        department=department,
        st_date=st_date,
        end_date=end_date,
    )

    # Get counts of categories appearing in the same transaction
    same_counts = get_same_counts(trans_df)

    # Get total counts of number of transactions each category was in
    tot_counts = get_tot_counts(trans_df)

    # Join same and total counts
    all_count_df = join_counts_dfs(same_counts, tot_counts)

    # Calculate expected counts of number of co-occurances of two categories based on their
    # independent penetration
    expected_counts = get_expected_counts(trans_df, all_count_df)

    # Calculate the association distances
    distances = calc_distances(expected_counts)

    # Append category descriptions
    distances = distances.withColumn("CAT_DESC_A", F.col("CAT_A")).withColumn(
        "CAT_DESC_B", F.col("CAT_B")
    )

    context.data.write(dataset_id="assoc_path", df=distances)
