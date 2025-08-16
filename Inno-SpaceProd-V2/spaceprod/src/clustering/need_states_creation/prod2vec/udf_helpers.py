from typing import Dict

import numpy as np
import pandas as pd

from inno_utils.loggers import log

# This object defines the schema of the final resulting dataframe that should
# come out of the model training UDF. This object is used for both
# the validation of schema data type conversion
MODEL_OUTPUT_PDF_SCHEMA = {
    "ITEM_NO": float,
    "index": int,
    "EMBEDD_NAME": str,
    "EMBEDD_VALUE": float,
    "EXEC_ID": str,
}


def parse_embeddings(
    item_embeddings_from_model: np.ndarray, reversed_dictionary: Dict[int, str]
) -> pd.DataFrame:
    """
    Parses embeddings as an output of the model into a Pandas dataframe

    Parameters
    ----------
    item_embeddings_from_model: numpy array of embeddings as it comes out
        of the model after .train

    reversed_dictionary: item dictionary which was an input to the model
        training UDF

    Returns
    -------
    pandas df with embeddings
    """

    pdf = pd.DataFrame(item_embeddings_from_model)

    # Create column headers
    cols = ["embedd_" + str(sub) for sub in pdf.columns]
    pdf.columns = cols
    pdf.loc[:, "index"] = pdf.index

    # Map the ITEM_NO
    pdf.loc[:, "ITEM_NO"] = pdf["index"].map(reversed_dictionary)

    return pdf


def prepare_embeddings_for_output(
    pdf_embeddings: pd.DataFrame,
) -> pd.DataFrame:
    """
    Processes the pandas representation of embeddings into a list of
    dictionaries in order to prepare the output of the UDF
    The output of this function will be a single cell in the resulting
    spark dataframe after UDF runs

    Parameters
    ----------
    pdf_embeddings

    Returns
    -------
    a tuple of: embeddings list and items list
    """

    pdf = pdf_embeddings

    # ensure ITEM_NO is consistently integer
    mask = pdf["ITEM_NO"] == "UNK"
    pdf["ITEM_NO"] = np.where(mask, -1, pdf["ITEM_NO"]).astype(np.float)

    return pdf


def prepare_exception_messages_for_output(
    exec_id: str,
    msg_exception: str,
    msg_traceback: str,
) -> pd.DataFrame:

    msg = f"EXCEPTION:\n{str(msg_exception)}\nTRACEBACK:\n{str(msg_traceback)}\n"

    pdf = pd.DataFrame(
        {
            "EXEC_ID": [str(exec_id)],
            "ITEM_NO": [float(0)],
            "index": [int(0)],
            "EMBEDD_NAME": [msg],
            "EMBEDD_VALUE": [float(0)],
        }
    )

    return pdf


def udf_out_processing(pdf_model_output: pd.DataFrame) -> pd.DataFrame:
    """
    Melts (wide/short to narrow/long) the model output to make sure embeddings
    are rows, not columns this is needed for standard return from UDF

    Parameters
    ----------
    pdf_model_output: pd.DataFrame
        wide output of the model

    Returns
    -------
    melted output of the model
    """

    # determine output columns
    cols_embed = [x for x in pdf_model_output.columns if x.startswith("embedd_")]

    # melt the result so that we don't have embedding names as columns
    # reason: the # of embeddings can vary and we need to return a standard
    # schema from the UDF
    pdf_result_melt = pd.melt(
        frame=pdf_model_output,
        id_vars=["ITEM_NO", "index"],
        value_vars=cols_embed,
        var_name="EMBEDD_NAME",
        value_name="EMBEDD_VALUE",
    )

    return pdf_result_melt


def udf_out_add_static_values(
    pdf: pd.DataFrame,
    exec_id: str,
) -> pd.DataFrame:
    """
    Adds remaining static values to the resulting output of the prod2vec
    model that will be used after spark collects UDF output.
    For e.g. EXEC_ID is needed to differentiate between model runs
    Parameters
    ----------
    pdf: outputs of prod2vec model
    exec_id: exec ID we are running the model for
    item_count: number of items the model runs for

    Returns
    -------
    processed model output
    """
    pdf["EXEC_ID"] = exec_id

    return pdf


def udf_out_type_casting(pdf: pd.DataFrame):
    """
    cstst the prod2vec output columns to be consistent with what spark
    expects downstream
    """
    for col_name, col_type in MODEL_OUTPUT_PDF_SCHEMA.items():
        log.info(f"start converting column 'col_name' to type '{str(col_type)}'")
        pdf[col_name] = pdf[col_name].astype(col_type)

    return pdf
