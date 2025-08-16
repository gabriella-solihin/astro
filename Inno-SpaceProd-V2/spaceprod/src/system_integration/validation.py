from functools import reduce
from operator import or_
from typing import List, Dict

from inno_utils.loggers.log import log
from spaceprod.utils.imports import F, SparkDataFrame


def ensure_clean_entity_relationship(
    cols: List[str],
    datasets_to_check: Dict[str, SparkDataFrame],
):
    """
    This function checks the relationship between multiple tables to ensure
    integrity on a given list of columns. For e.g. when you
    want to check that all datasets you are working with have consistent
    set of stores or consistent set of items or any other entities.
    Basically it catches "missing" entities (i.e. missing stores/items/etc)

    NOTE: it does not simply check counts, it actually checks the the VALUES
    match between datasets by doing an outer join across all given datasets
    """

    # first ensure all given datasets have the required entity keys
    for name_df, df in datasets_to_check.items():
        list_check = list(set(cols) - set(df.columns))
        msg = f"dataset '{name_df}' is missing entitiy column(s): {list_check}"
        assert len(list_check) == 0, msg

    # now only take the entity keys and the unique combinations between them
    list_dfs = [
        df.select(*cols).dropDuplicates().withColumn(f"entity_in_{name_df}", F.lit(1))
        for name_df, df in datasets_to_check.items()
    ]

    # do an outer join between all DFs
    df_joined = reduce(lambda df1, df2: df1.join(df2, cols, "outer"), list_dfs)
    df_joined = df_joined.fillna(0)

    # find the gaps in the outer join that will represent missing entities
    cols_check = [x for x in df_joined.columns if not x in cols]
    mask = reduce(or_, [F.col(x) == 0 for x in cols_check])
    df_check = df_joined.filter(mask)

    # construct message and raise error if needed
    cols_check = [x for x in df_joined.columns if not x in cols]
    dict_summary = {x: df_joined.filter(F.col(x) == 1).count() for x in cols_check}
    msg_summary = f"\nEntities checked: {cols}\nEntity counts: {dict_summary}"
    msg = f"\nInconsistent entities found across datasets: {msg_summary}"
    assert df_check.limit(1).count() == 0, msg
    log.info(f"\nChecked entity relationship and it's good: {msg_summary}")


def validate_replication(
    df: SparkDataFrame,
    dims=List[str],
    replicated_columns=List[str],
    agg_dim=List[str],
):
    """
    This function checks the correctness of replication

    Parameters
    ----------
    df: SparkDataFrame
    dims: list
        list of columns which can be used as for grouping records
    replicated_columns: list
        list of columns which are replicated and can be used to find distinct record
    agg_dim: list
        column lists which can be used to find distinct store
    """
    df_agg = df.groupBy(*dims).agg(
        F.countDistinct(*replicated_columns).alias("distinct_record_count"),
        F.countDistinct(*agg_dim).alias("distinct_store_count"),
    )
    dd_agg_mul = df_agg.withColumn(
        "total_record",
        (F.col("distinct_record_count")) * (F.col("distinct_store_count")),
    )

    msg = f"there are duplicates and please check replication"
    assert dd_agg_mul.count() == dd_agg_mul.dropDuplicates([*dims]).count(), msg

    msg = f"count not matching,check replication"
    assert dd_agg_mul.agg(F.sum("total_record")).collect()[0][0] == df.count(), msg
