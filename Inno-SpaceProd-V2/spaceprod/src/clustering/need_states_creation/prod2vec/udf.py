import traceback
from typing import Dict, List, Callable

import pandas as pd

from inno_utils.loggers import log

from spaceprod.src.clustering.need_states_creation.prod2vec.modelling import (
    build_model,
    compile_model,
    train_model,
)
from spaceprod.src.clustering.need_states_creation.prod2vec.udf_helpers import (
    udf_out_type_casting,
    udf_out_add_static_values,
)
from spaceprod.utils.data_transformation import get_single_value
from spaceprod.utils.imports import F, T

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", 12)
pd.set_option("max_columns", None)
pd.set_option("expand_frame_repr", None)


def fit_prod2vec_udf(
    exec_id: str,
    all_basket_data: List[List[List[int]]],
    reversed_dictionary: Dict[int, str],
    batch_size: int,
    num_ns: int,
    max_num_items: int,
    embedding_size: int,
    item_embeddings_layer_name: str,
    num_epochs: int,
    steps_per_epoch: int,
    early_stopping_patience: int,
):
    """This a UDF function that fits the actual prod to vector model for the
    items in a need states for a given category
    TODO: document args in this docstring
    """

    # perform required imports locally as this is required for UDF
    from inno_utils.loggers import log
    from spaceprod.src.clustering.need_states_creation.prod2vec.udf_helpers import (
        parse_embeddings,
        prepare_embeddings_for_output,
    )
    from spaceprod.src.clustering.need_states_creation.prod2vec.modelling import (
        generate_training_data,
    )
    from spaceprod.src.clustering.need_states_creation.prod2vec.udf_helpers import (
        udf_out_processing,
    )

    import inspect

    # spit the inputs to the function
    # they will appear in Spark workers logs
    # (this is useful for debugging and traceability)
    locs = locals()
    full_arg_spec = inspect.getfullargspec(fit_prod2vec_udf)
    arg_values = {i: locs[i] for i in full_arg_spec.args}
    msg_inputs = "".join([f"{k}={repr(v)}\n" for k, v in arg_values.items()])
    log.info(f"Calling modeling with following inputs:\n{msg_inputs}\n")

    log.info(f"Start running prod2vec model for Exec ID: '{exec_id}'")

    num_prods = len(reversed_dictionary)
    log.info(f"Number of products found: {num_prods}")

    data_generator = generate_training_data(
        batch_size=batch_size,
        sequences=all_basket_data,
        num_ns=num_ns,
        num_prods=num_prods,
        seed=123,
        max_num_pairs=max_num_items,
        shuffle=True,
    )

    log.info(f"Created generator data for: '{exec_id}'.")

    _model = build_model(
        num_prods=num_prods,
        embedding_size=embedding_size,
        item_embeddings_layer_name=item_embeddings_layer_name,
        num_ns=num_ns,
    )

    log.info(f"Built prod2vec model for: '{exec_id}'.")

    _model = compile_model(model=_model)

    log.info(f"Compiled prod2vec model for: '{exec_id}'.")

    log.info("========== Train model ==========")

    history, model = train_model(
        model=_model,
        data_generator=data_generator,
        num_epochs=num_epochs,
        steps_per_epoch=steps_per_epoch,
        early_stopping_patience=early_stopping_patience,
        reversed_dictionary=reversed_dictionary,
    )

    log.info(f"Trained prod2vec model for: '{exec_id}'.")

    ###########################################################################
    # HANDLING OUTPUTS OF THE train_model METHOD
    ###########################################################################

    # obtain elapsed_epochs
    elapsed_epochs = len(history.history["loss"])

    log.info(f"Elapsed # of epochs: {elapsed_epochs}")

    if not early_stopping_patience:
        assert elapsed_epochs == num_epochs, "not all epochs elapsed"

    # obtain and parse embeddings data
    from spaceprod.utils.model_helper import _item_embeddings_from_model

    item_embeddings_from_model = _item_embeddings_from_model(_model)

    pdf_embeddings = parse_embeddings(
        item_embeddings_from_model=item_embeddings_from_model,
        reversed_dictionary=reversed_dictionary,
    )

    log.info(f"Parsed embeddings for: '{exec_id}'.")

    pdf_model_output = prepare_embeddings_for_output(
        pdf_embeddings=pdf_embeddings,
    )

    log.info(f"Prepared output for: '{exec_id}'.")
    log.info(f"Output contains: {len(pdf_model_output)} rows (items).")

    # melt the result so that we don't have embedding names as columns
    # reason: the # of embeddings can vary and we need to return a standard
    # schema from the UDF
    log.info(f"Starting melting data for: {exec_id}")
    pdf_result_melt = udf_out_processing(
        pdf_model_output=pdf_model_output,
    )

    # Adds remaining static values to the resulting output of the prod2vec
    # model that will be used after spark collects UDF output.
    log.info(f"Starting adding static values to data for: {exec_id}")
    pdf_result_values = udf_out_add_static_values(
        pdf=pdf_result_melt,
        exec_id=exec_id,
    )

    log.info(f"Starting casting data for: {exec_id}")
    pdf_result_casted = udf_out_type_casting(
        pdf=pdf_result_values,
    )

    log.info(f"Starting returning output data for: {exec_id}")

    return pdf_result_casted


def determine_timeout_for_modelling(num_epochs: int) -> int:
    """
    This function attempts to identify a proper timeout for the model training
    to prevent the 'locking' and having udf running "forever"
    PLEASE NOTE: in case if this timeout is ever reached, the pipeline
    raises and assertion error. So we can be confident that we never get
    "silent errors"

    Parameters
    ----------
    num_epochs: number of epochs that is running for the current model run

    Returns
    -------
    a timeout in minutes
    """

    # an arbitrary number that represents "max" run time per epoch (minutes)
    per_epoc_min = 1.2

    # can never be less than 10 minutes
    min_timeout_min = 10

    timeout_min = max(num_epochs * per_epoc_min, min_timeout_min)

    timeout_min = int(round(timeout_min, 0))

    # TODO: we cannot use max for this, implement a better formula
    max_timeout_min = 120
    timeout_min = min(max_timeout_min, timeout_min)

    return timeout_min


def fit_prod2vec_udf_wrapper(*args, **kwargs):
    """
    A wrapper for error handling,
    TODO: Currently just catches ANY types of exceptions in the same way.
     in future this may evolve into a more complex error handling logic.
    """

    exec_id = kwargs["exec_id"]
    log.info(f"Start running 'fit_prod2vec_udf_wrapper' for '{exec_id}'")

    # determine timeout
    num_epochs = kwargs["num_epochs"]
    timeout_min = determine_timeout_for_modelling(num_epochs=num_epochs)
    log.info(f"Setting modelling timeout (minutes): {timeout_min}")

    # import dependencies locally
    from spaceprod.src.clustering.need_states_creation.prod2vec.udf_helpers import (
        prepare_exception_messages_for_output,
    )

    from spaceprod.src.clustering.need_states_creation.prod2vec.helpers import (
        TimeOutForModel,
    )

    timeout = TimeOutForModel(timeout_min)

    try:

        pdf_model_output = fit_prod2vec_udf(*args, **kwargs)

        timeout.cancel()

        return pdf_model_output

    except Exception as ex:

        # in case if it breaks collect the stack trace and errors to
        # provide most informative errors from each worker
        tb = traceback.format_exc()

        pdf_error = prepare_exception_messages_for_output(
            exec_id=exec_id,
            msg_exception=str(ex),
            msg_traceback=str(tb),
        )

        timeout.cancel()

        return pdf_error


def generate_prod2vec_udf(
    batch_size: int,
    num_ns: int,
    max_num_pairs: int,
    embedding_size: int,
    item_embeddings_layer_name: str,
    num_epochs: int,
    steps_per_epoch: int,
    early_stopping_patience: int,
    path_prod2vec_staged_data: str,
) -> Callable:
    """
    Generates the UDF function to be used to train the prod2vec model
    Takes all the necessary static parameters that are being passed to
    the UDF as globals. This way we don't need to physically pass them through
    UDF.

    Parameters
    ----------
    batch_size: int
      Size of the batch to yield

    num_ns : int
        The number of negative samples to select per positive sample

    num_prods : int
        The number of unique products in the product dictionary

    max_num_pairs : int
        The maximum number of items to consider for each customer, used to speed up training

    embedding_size: int
        TODO

    item_embeddings_layer_name: str
        TODO

    num_epochs:
        number of epochs that is running for the current model run

    steps_per_epoch: int
        number of batches to train on that is considered an epoch

    early_stopping_patience: int
        number of epochs without improvement in loss before stopping training

    window_size: int
        TODO

    path_prod2vec_staged_data: str
        was used to save model as pickle. Will be removed as not used atm.

    Returns
    -------
    UDF function to be called
    """

    # This is the schema if the dataset that is expected to be produced by
    # the prod2vec wrapper UDF function
    schema_out_prod_to_vec = T.StructType(
        [
            T.StructField("EXEC_ID", T.StringType(), True),
            T.StructField("ITEM_NO", T.FloatType(), True),
            T.StructField("index", T.IntegerType(), True),
            T.StructField("EMBEDD_NAME", T.StringType(), True),
            T.StructField("EMBEDD_VALUE", T.FloatType(), True),
        ]
    )

    @F.pandas_udf(schema_out_prod_to_vec, F.PandasUDFType.GROUPED_MAP)
    def udf(
        pdf: pd.DataFrame,
    ):
        """
        UDF function that will be used to train the prod2vec model for
        Need States module.

        We are using a Pandas UDF here because it is a good use case for it,
        i.e. we feed non-aggregated data and return back also non-aggregated
        data (both in Pandas DF format).
        This way we don't need to pre-aggregate data into lists/dicts.

        Parameters
        ----------
        pdf: pd.DataFrame
         pandas dataframe that will be automatically generated
         when the UDF is called using .apply function in PySpark

        Returns
        -------
        model results in pd.DataFrame format
        """
        from spaceprod.utils.data_helpers import read_blob_inside_worker

        from spaceprod.src.clustering.need_states_creation.prod2vec.helpers import (
            create_data_for_prod2vec_pairs,
        )

        # get constants that are same for every EXEC_ID run
        exec_id = get_single_value(pdf, "EXEC_ID")

        # read the input data from the pre-configured path
        # see explanation here:
        # spaceprod.src.clustering.need_states_creation.prod2vec.helpers.call_prod2vec_model_udf
        path_parquet = f"{path_prod2vec_staged_data}/EXEC_ID=" + str(exec_id)
        pdf_input_data = read_blob_inside_worker(path_parquet)

        (
            all_basket_data,
            count,
            dictionary,
            reversed_dictionary,
        ) = create_data_for_prod2vec_pairs(
            pdf=pdf_input_data,
        )

        pdf_output = fit_prod2vec_udf_wrapper(
            exec_id=exec_id,
            all_basket_data=all_basket_data,
            reversed_dictionary=reversed_dictionary,
            batch_size=batch_size,
            num_ns=num_ns,
            max_num_items=max_num_pairs,
            embedding_size=embedding_size,
            item_embeddings_layer_name=item_embeddings_layer_name,
            num_epochs=num_epochs,
            steps_per_epoch=steps_per_epoch,
            early_stopping_patience=early_stopping_patience,
        )

        return pdf_output

    return udf
