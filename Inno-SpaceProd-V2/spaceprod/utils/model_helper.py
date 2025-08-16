import tensorflow as tf
import numpy as np
import pandas as pd

from abc import ABCMeta

from inno_utils.loggers import log
from tensorflow.keras.layers import Embedding, Dot, Flatten, Input
from tensorflow.keras import Model
from tensorflow.keras.callbacks import Callback, EarlyStopping, ModelCheckpoint
from tensorflow.keras.models import load_model
from tensorflow.keras.optimizers import SGD, Adam, RMSprop
from tensorflow.keras.utils import plot_model

import h5py
from sklearn.metrics.pairwise import cosine_similarity

# global parameters for prod vec definition - TODO move to config
item_embeddings_layer_name = "p2v_embedding"
valid_window = 20
valid_size = 20


class Prod2Vec(object):
    __metaclass__ = ABCMeta

    def __init__(
        self,
        reversed_dictionary,
        num_prods,
        item_embeddings_layer_name,
        num_ns,
        embedding_size,
        run_similarity_qa=True,
        similarity_period=1,
    ):

        self._num_prods = num_prods
        self._embedding_size = embedding_size
        self._item_embeddings_layer_name = item_embeddings_layer_name
        self._num_ns = num_ns
        self._model = None
        self._reversed_dictionary = reversed_dictionary
        self.run_similarity_qa = run_similarity_qa
        self.similarity_period = similarity_period

    @property
    def item_embeddings(self):
        return _item_embeddings_from_model(self._model)

    def build(self):
        raise NotImplementedError()

    def compile(self, optimizer=None):
        if not optimizer:
            # default value
            optimizer = RMSprop(lr=0.0001)

        self._model.compile(
            optimizer=optimizer,
            loss=tf.keras.losses.CategoricalCrossentropy(from_logits=True),
            metrics=["categorical_accuracy"],
        )

    def train(
        self,
        generator,
        steps_per_epoch=1000,  # default values
        epochs=100,  # default values
        early_stopping_patience=None,
        save_path=None,
        save_period=None,
        save_item_embeddings_path=None,
        save_item_embeddings_period=None,
    ):
        """
        this function when called executes the training run for the Prod2Vec run

        Parameters
        ----------
        generator: generator object
            generator that creates batches of target and context pairs for training the model
        steps_per_epoch: int
            number of batches to train on that is considered an epoch
        epochs: int
            number of epochs
        early_stopping_patience: str
            number of epochs without improvement in loss before stopping training
        save_path: str
            path to save the trained model
        save_period: str
            number of epochs between saving
        save_item_embeddings_path: str
            path to save the fitted item embeddings
        save_item_embeddings_period: str
            number of epochs between saving embeddings

        Returns
        --------
        history: tf model fit object
            object containing all metrics contained in the training function, i.e. loss
        """

        callbacks = []
        # TODO: this is the step where most model runs freeze indefinitely
        # stop the model training if no improvement in loss over specified number of epochs
        if early_stopping_patience:
            callbacks.append(
                EarlyStopping(monitor="loss", patience=early_stopping_patience)
            )

        # saves the model and item embeddings every x epochs
        if save_path and save_period:
            callbacks.append(ModelCheckpoint(save_path, period=save_period))
        if save_item_embeddings_path and save_item_embeddings_period:
            callbacks.append(
                _SaveItemEmbeddings(
                    save_item_embeddings_path, save_item_embeddings_period
                )
            )

        # used for QA purposes; prints to the log examples of similar items from the trained model
        # can specify the number of epochs between running this check. Should see similar items become more
        # intuitive as the model learns
        if self.similarity_period and self.run_similarity_qa:
            callbacks.append(
                _TopKSimilarity(
                    period=self.similarity_period,
                    reversed_dictionary=self._reversed_dictionary,
                )
            )

        history = self._model.fit(
            generator,
            callbacks=callbacks,
            steps_per_epoch=steps_per_epoch,
            epochs=epochs,
        )

        return history

    def save(self, path):
        print("Saving model to %s", path)
        self._model.save(path)

    def save_item_embeddings(self, path):
        _write_item_embeddings(self.item_embeddings, path)

    def load(self, path):
        print("Loading model from %s", path)
        self._model = load_model(path)


class _SaveItemEmbeddings(Callback):
    def __init__(self, path, period):
        self.path = path
        self.period = period

    def on_epoch_end(self, epoch, logs=None):
        if epoch % self.period != 0:
            return

        path = self.path.format(epoch=epoch)
        print("Saving item embeddings to {}".format(path))
        embeddings = _item_embeddings_from_model(self.model)
        _write_item_embeddings(embeddings, path)


def _item_embeddings_from_model(keras_model):
    for layer in keras_model.layers:
        if layer.get_config()["name"] == item_embeddings_layer_name:
            return layer.get_weights()[0]


def _write_item_embeddings(item_embeddings, path):
    print("Saving item embeddings to %s", path)
    with h5py.File(path, "w") as f:
        f.create_dataset(item_embeddings_layer_name, data=item_embeddings)


class _TopKSimilarity(Callback):
    """Will run simililarity QA checks after each epoch"""

    def __init__(self, period, reversed_dictionary):
        self.period = period
        self.reversed_dictionary = reversed_dictionary

    def on_epoch_end(self, epoch, logs=None):
        if epoch % self.period != 0:
            return

        embeddings = _item_embeddings_from_model(self.model)
        _run_sim(
            embed_weights=embeddings,
            valid_window=valid_window,
            valid_size=valid_size,
            reversed_dictionary=self.reversed_dictionary,
        )


def _run_sim(embed_weights, valid_window, valid_size, reversed_dictionary):
    """Creates top k nearest products for each validation target product - utilized in model callback

    Parameters
    ----------
    embed_weights: list
      Item embedding weights
    valid_window: int
      Randomly choose from the top {valid_window} products for similarity checks
    valid_size: int
      The number of products to get similarity for
    dictionary: dict
      Mapping of ITEM_NO to product description
    reversed_dictionary: dict
      Mapping of product index to ITEM_NO

    """

    product_keys = list(reversed_dictionary.keys())
    size_to_sample = min(valid_size, len(product_keys))
    valid_examples = np.random.choice(product_keys, size_to_sample, replace=False)

    log.info(f"Valid examples generated: {list(valid_examples)}")
    log.info(f"Reversed dictionary: {reversed_dictionary}")

    for i in range(size_to_sample):

        # Get the product to validate and obtain its embedding
        valid_prod = reversed_dictionary[valid_examples[i]]
        valid_embed = embed_weights[valid_examples[i]].reshape(1, -1)

        # Calculate cosine similarity between the validation product and all other prods
        cosine_sim = cosine_similarity(embed_weights, Y=valid_embed, dense_output=True)
        cosine_sim_pd = pd.DataFrame(cosine_sim)
        cosine_sim_pd.columns = ["cosine_sim"]

        # Get the products that are most similar to the validation product
        cosine_sim_pd.loc[:, "index"] = cosine_sim_pd.index
        cosine_sim_pd.sort_values("cosine_sim", ascending=False, inplace=True)
        cosine_sim_pd = cosine_sim_pd[cosine_sim_pd["index"] != valid_examples[i]]
        cosine_sim_pd.loc[:, "valid_prod"] = cosine_sim_pd["index"].map(
            reversed_dictionary
        )

        # cosine_sim_pd.loc[:, "prod_desc"] = cosine_sim_pd["valid_prod"].map(dictionary)
        ## Write out the product descriptions
        # valid_desc = dictionary.get(valid_prod)
        # nearest_desc = cosine_sim_pd["prod_desc"][:20].str.cat(sep="; ")
        # print("Nearest to {}: {}".format(valid_desc, nearest_desc))


class ProdToVecModel(Prod2Vec):
    def build(self):
        """Defines the prod2vec model architecture"""

        contexts = Input(shape=(None, self._num_ns + 1, 1))
        targets = Input(shape=(None,))

        # Embedding for the target product
        target_embedding = Embedding(
            self._num_prods,
            self._embedding_size,
            input_length=1,
            name=self._item_embeddings_layer_name,
        )(targets)

        # Embedding for the context product
        context_embedding = Embedding(
            self._num_prods, self._embedding_size, input_length=self._num_ns
        )(contexts)
        # context_embedding = tf.squeeze(context_embedding)

        # Dot product similarity
        dots = Dot(axes=(2, 4))([target_embedding, context_embedding])
        dots_flatten = Flatten()(dots)

        self._model = Model(inputs=[targets, contexts], outputs=dots_flatten)
