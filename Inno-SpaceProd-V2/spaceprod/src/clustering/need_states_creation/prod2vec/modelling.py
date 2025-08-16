import random as rnd
from typing import Union, List, Optional, Dict, Generator, Tuple
from tensorflow.keras.optimizers import RMSprop
import numpy as np
import tensorflow as tf
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.python.keras.callbacks import History

from spaceprod.utils.model_helper import _TopKSimilarity
from tensorflow.keras.layers import Embedding, Dot, Flatten, Input
from tensorflow.keras import Model


def generate_training_data(
    batch_size: int,
    sequences: Union[List[List[List[int]]], List[List[int]]],
    num_ns: int,
    num_prods: int,
    seed: int,
    max_num_pairs: int,
    shuffle: Optional[bool] = True,
) -> Generator:
    """Generator function to yield batches of training data containing lists of target products, context products and labels

    Parameters
    ----------
    batch_size: int
      Size of the batch to yield
    sequences : list
        List where each element is a list of items purchased by 1 customer
    num_ns : int
        The number of negative samples to select per positive sample
    num_prods : int
        The number of unique products in the product dictionary
    seed : int
        The seed for the negative sampling
    max_num_pairs : int
        The maximum number of items to consider for each customer, used to speed up training
    shuffle : bool
      Controls whether the sequence of customers should be suffled between batches (defaults to True)

    Returns
    -------
    targets : list
        List of target products
    contexts : list
        List of context products (including negative samples)
    labels : list
        Binary 0/1 indicator if the target / context pair were a positive sample (1) or negative sample (0)

    """

    # Initialize index
    # Needs to starts at 0 to start at the first customer in the file
    index = 0

    # initialize current batch lists
    targets, contexts, labels = [], [], []

    # Initialize batch count
    batch_count = 0

    # count the number of customers in the file
    num_custs = len(sequences)

    # create an array with the indexes of customers that can be shuffled
    cust_index = [*range(num_custs)]

    # shuffle customer indexes if shuffle is set to True
    if shuffle:
        rnd.shuffle(cust_index)

    while True:
        # if the index is greater or equal than to the number of customers in the data
        if index >= num_custs:
            # then reset the index to 0
            index = 0
            # shuffle line indexes if shuffle is set to True
            if shuffle:
                rnd.shuffle(cust_index)

        # Get the list of items purchased for the selected customer
        target_context_pairs = sequences[cust_index[index]]

        # shuffle the items in the basket
        rnd.shuffle(target_context_pairs)

        # cap the maximum number of items for each customer (to speed up training)
        target_context_pairs = target_context_pairs[0:max_num_pairs]

        # Iterate over each positive skip-gram pair to produce training examples
        # with positive context word and negative samples.
        for target_prod, context_prod in target_context_pairs:

            context_class = tf.expand_dims(
                input=tf.constant([context_prod], dtype="int64"),
                axis=1,
            )

            # use of log_uniform sampler is done to ensure a good representation of less popular items
            # in the training data
            (
                negative_sampling_candidates,
                _,
                _,
            ) = tf.random.log_uniform_candidate_sampler(
                true_classes=context_class,
                num_true=1,
                num_sampled=num_ns,
                unique=True,
                range_max=num_prods,
                seed=seed,
                name="negative_sampling",
            )

            # Build context and label vectors (for one target product)
            negative_sampling_candidates = tf.expand_dims(
                negative_sampling_candidates, 1
            )

            context = tf.concat([context_class, negative_sampling_candidates], 0)
            # creates a vector where the first position takes the value of 1 to represent the positive sample
            # the remaining positions are filled with 0s to represent the label with negative samples
            label = tf.constant([1] + [0] * num_ns, dtype="int64")

            # Append each element from the training example to global lists.
            targets.append(target_prod)
            contexts.append(context)
            labels.append(label)

        index += 1
        batch_count += 1

        # if the current batch is now equal to the desired batch size
        if batch_count == batch_size:
            if len(np.array(targets)) > 0:
                yield [np.array(targets), np.array(contexts)], np.array(labels)
            targets, contexts, labels = [], [], []
            batch_count = 0


def build_model(
    num_prods: int,
    embedding_size: int,
    item_embeddings_layer_name: str,
    num_ns: int,
) -> Model:
    """Defines the prod2vec model architecture"""

    contexts = Input(shape=(None, num_ns + 1, 1))
    targets = Input(shape=(None,))

    # Embedding for the target product
    target_embedding = Embedding(
        input_dim=num_prods,
        output_dim=embedding_size,
        input_length=1,
        name=item_embeddings_layer_name,
    )(targets)

    # Embedding for the context product
    context_embedding = Embedding(
        input_dim=num_prods,
        output_dim=embedding_size,
        input_length=num_ns,
    )(contexts)

    # Dot product similarity
    dots = Dot(axes=(2, 4))([target_embedding, context_embedding])
    dots_flatten = Flatten()(dots)

    _model = Model(inputs=[targets, contexts], outputs=dots_flatten)

    return _model


def compile_model(model: Model) -> Model:
    """Compiles model (Configures the model for training)"""

    optimizer = RMSprop(lr=0.0001)
    model.compile(
        optimizer=optimizer,
        loss=tf.keras.losses.CategoricalCrossentropy(from_logits=True),
        metrics=["categorical_accuracy"],
    )

    return model


def train_model(
    model: Model,
    data_generator: Generator,
    num_epochs: int,
    steps_per_epoch: int,
    early_stopping_patience: int,
    reversed_dictionary: Dict[int, str],
) -> Tuple[History, Model]:
    """
    this function executes the training run for the Prod2Vec run

    Parameters
    ----------

    model: Model
        built and compiled model for prod2vec

    data_generator: generator object
        generator that creates batches of target and context pairs for training
        the model

    steps_per_epoch: int
        number of batches to train on that is considered an epoch

    num_epochs: int
        number of epochs

    early_stopping_patience: str
        number of epochs without improvement in loss before stopping training

    Returns
    --------

    history: tf model fit object
        object containing all metrics contained in the training function, i.e. loss

    model: Model
        trained model

    """

    callbacks = []
    # TODO: this is the step where most model runs freeze indefinitely
    # stop the model training if no improvement in loss over specified number of epochs
    if early_stopping_patience:
        callbacks.append(
            EarlyStopping(monitor="loss", patience=early_stopping_patience)
        )

    # used for QA purposes; prints to the log examples of similar items from the trained model
    # can specify the number of epochs between running this check. Should see similar items become more
    # intuitive as the model learns
    callbacks.append(
        _TopKSimilarity(
            period=1,
            reversed_dictionary=reversed_dictionary,
        )
    )

    history = model.fit(
        x=data_generator,
        callbacks=callbacks,
        steps_per_epoch=steps_per_epoch,
        epochs=num_epochs,
    )

    return history, model
