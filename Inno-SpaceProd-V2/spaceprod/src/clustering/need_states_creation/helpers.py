from typing import Dict

from spaceprod.utils.imports import SparkDataFrame, F


def validate_dataset(df: SparkDataFrame, data_contract: Dict[str, str]):
    """
    a simple validation that only checks for columns and renames accordingly
    """

    cols = [F.col(k).alias(v) for k, v in data_contract.items()]
    df = df.select(*cols)
    return df
