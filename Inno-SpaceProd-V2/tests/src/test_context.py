"""
Any context-related tests
TODO: we could add more comprehensive tests here with edge cases
"""
import pytest


@pytest.mark.skip  # TODO: address: long test
def test_basic_context_features():
    """
    a very basic test to make sure we can parse all our YMLs and
    create a new context (isn't covered by our integ test because it
    uses a mock config which is completely independent of our YML configs)

    Read more about context here: https://sobeysdigital.atlassian.net/wiki/spaces/SP/pages/5444370468/Accessing+Data+Config+and+Run+Context
    """

    from spaceprod.utils.space_context import context

    # ensure all key attributes are accessible
    assert context.config is not None
    assert context.run_id is not None
    assert context.run_folder is not None

    # try dumping the session
    context.dump_session()

    # try reading the session from the run folder
    context.reload_context_from_run_folder(context.run_folder)

    # try reloading YMLs from spaceprod library
    context.reload_config_from_disc()

    # try cloning and make sure you get new run ID
    run_id = context.run_id
    run_folder = context.run_folder
    context.clone_run("test_clone")
    assert run_id != context.run_id
    assert run_folder != context.run_folder

    # try reading/writing data

    # first add a fake dataset and write to it
    context.config["clustering"]["concatenate_clustering_config"][
        "concatenate_clustering_outputs"
    ]["test_file_write"] = "sobeys_space_prod/{run_id}/test_file_write"

    # create a DF to write/read
    from tests.utils.spark_util import spark_tests as spark

    # write the DF and read it back
    df_write = spark.createDataFrame([{"A": 0}])
    context.data.write(df=df_write, dataset_id="test_file_write")
    df_read = context.data.read("test_file_write")
    assert df_write.exceptAll(df_read).limit(1).count() == 0
