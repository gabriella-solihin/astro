import os

import click

from azure.storage.blob import BlockBlobService
from inno_utils.cli import install_lib_on_databricks, upload_release_file_to_blob
from inno_utils.packaging import create_whl
from inno_utils.git import get_current_branch

# NOTE : I've left this command group structure as a template
# in case we want to add more commands later; cli() is the common
# entrypoint into all commands
@click.group()
@click.pass_context
def cli(ctx):
    """
    *** SPACEPROD CLI ***

    Making routine spaceprod tasks a little easier.

    To learn more about any given command, call that command
    followed by the `--help` flag.

    """
    print("\n*******************" "\n*** SPACEPROD CLI ***" "\n*******************")

    # ensure that ctx.obj exists and is a dict (in case `cli()` is called
    # by means other than the main() function block below)
    ctx.ensure_object(dict)
    # if you had shared options or variables, you'd pass them to
    # commands as such:
    # ctx.obj["cluster_id"] = cluster_id


@cli.command()
@click.option(
    "--cluster-id",
    "-c",
    default=None,
    help="Overrides your local CLUSTER_ID env variable with a custom cluster id.",
)
@click.option(
    "-R",
    "--remove-old",
    is_flag=True,
    default=False,
    help=(
        "Removes previously-installed spaceprod libs (for clean-up only; "
        "not required for your newly-installed lib to work; databricks "
        "will use the latest spaceprod lib by default)."
    ),
)
@click.pass_context
def install_on_db(ctx, cluster_id, remove_old):
    """Installs your local spaceprod code onto databricks.

    By default, it installs to the cluster id specified by
    your local environment variable `CLUSTER_ID`.
    You'll probably want this to match the cluster id specified in
    your databricks-connect configuration (so that your installs
    and runs are on the same cluster).

    You must call this command from the project's root directory
    (i.e. from the same dir that contains `setup.py`).

    You can override the default and install to a different cluster
    by passing that cluster's id with the `-c` option.

    """

    install_lib_on_databricks(cluster_id, "spaceprod", remove_old)


@cli.command()
@click.pass_context
def upload_to_blob(ctx):
    """
    Uploads the wheel file and runner files to Azure Dev Blob for testing.

    This command must be executed in the project root (i.e when you do `ls` you should see setup.py)

    The files are located in the dev blob at `adhoc/$USERNAME/libraries`.
    """

    whl_file = create_whl(os.getcwd())

    user = os.environ["USER"]

    file_name = f"adhoc/{user}/libraries/spaceprod/{whl_file.split('/')[-1]}"
    upload_release_file_to_blob(whl_file, file_name)


@cli.command()
@click.pass_context
def upload_to_prod(ctx):
    """
    Uploads the wheel file and runner files to production library folder to be used by our jobs.

    This command should be used for hot fixes.

    This command must be executed in the project root (i.e when you do `ls` you should see setup.py)
    """

    raw_input = input(
        "Are you sure you want to upload this code, to be used in prod? [y/n] "
    )

    if raw_input != "y":
        return "Exiting"

    if get_current_branch() != "master":
        raw_input = input(
            "You are not on the master branch. Are you sure you want to continue? [y/n] "
        )

        if raw_input != "y":
            return "Exiting"

    if not os.environ["PROD_SAS_TOKEN"]:
        print("`PROD_SAS_TOKEN` not found.")

    whl_file = create_whl(os.getcwd())

    blob = BlockBlobService(
        account_name="sbyccprdatabricksmigrsa", sas_token=os.environ["PROD_SAS_TOKEN"]
    )

    file_name = f"libraries/spaceprod/libs/{whl_file.split('/')[-1]}"
    upload_release_file_to_blob(whl_file, file_name, blob)


def main():
    cli()
