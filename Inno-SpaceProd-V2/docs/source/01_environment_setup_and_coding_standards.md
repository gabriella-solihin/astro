
# Dev Environment Setup 

Follow the bellow steps to perform a 1-time setup of your dev environment on your local laptop.

This assumes you are inside your PROJECT directory (this is not same as
REPO directory).

E.g. your PROJECT directory can be: `/home/john/sobeys_projects` and
therefore your Space Prod REPO directory must be
`/home/john/sobeys_projects/space_prod`.

Your virtual env must live here: `/home/john/sobeys_projects/inno_venv`
(NOT inside your repo directory).

1.  Clone the repository in your PROJECT directory, therefore
    `git clone <link> /home/john/sobeys_projects/space_prod`

2.  Make sure you have access to Inno-Utils repository (will be accessed
    by our `setup.py` script during installation as we depend on it):
    <https://dev.azure.com/SobeysInc/_git/Inno-Utils>

3.  Make sure you have elevated privileges (start your command line as
    Administrator on Windows)

4.  If you are on Windows please use Cygwin, Git bash, or similar (Full
    complete installation of Cmder is recommended: <https://cmder.net/>

5.  Make sure you are in your PROJECT directory:
    `cd /home/john/sobeys_projects`

6.  Make sure you start without a virtual environment activated:
    `deactivate` (if any)

7.  Make sure you start with a fresh virtual environment, delete old if
    it exists: `rm -rf inno_venv`

8.  Check your Python version (must be 3.7.x): `python --version` (it is
    NOT recommended to use Anaconda, please download Python from
    official Python website)

9.  Create the virtual environment: `python3.7 -m venv inno_venv`
    - Please note, if you are on Mac OS, you might run into some issues later when you install the spaceprod package in step 14. Consider using Anaconda environment here. However, we have seen regular Python venv work on Mac OS as well, all this depends on your system's configuration. To follow this instructions for Anaconda environment, please refer to the official Anaconda docs.
    
10. Activate the virtual environment:
    `source inno_venv/Scripts/activate` (Windows) or
    `source inno_venv/bin/activate` (Linux, Mac)

11. Make sure `which pip` and `which python` are both pointing to your
    virtual env location

12. Make sure you have the compatible version of `pip`, run the
    following command to install pip version 20.2.3:
    `pip install -U pip==20.2.3`

13. Install `wheel` package manually (for some reason is not picked up
    during installation): `pip install wheel`

14. Installing Sobeys Space Prod package:
    `python -m pip install -e space_prod`

    -   If you get an error that Microsoft Visual C++ Build Tools are
        required, download them from here:
        <https://visualstudio.microsoft.com/visual-cpp-build-tools/> and
        make sure you select \"Build Tools\" when prompted during the
        installation

    -   If you get an error that it cannot access a temp directory due
        to a permissions issue, try telling pip which build directory to
        use, for e.g. this will create a local `inno_build` folder (make
        sure its not inside the repo!!!):
        `python -m pip install -b inno_build -e space_prod`

        -   if this does not help, try changing the permissions on your
            temp directory accordingly and/or try adding `--user` option
            to your command. If it throws an error that says "*Can not
            perform a \'\--user\' install. User site-packages are not
            visible in this virtualenv*",try this:

            -   Go to the `pyvenv.cfg` file in the Virtual environment
                folder

            -   Set the `include-system-site-packages` to `true` and
                save the change

            -   Reactivate the virtual environment.

    -   if you get an error that it cannot access your venv folder, make
        sure that you don't have any active python processes / consoles
        / notebooks running. Close all apps that might use python.

15. Configure your databricks connect (already installed as part of
    package dependencies if you followed steps above, you just need to
    configure it). To configure it, you can refer to the official
    documentation bellow, but basically it just comes down to running
    command: `databricks configure --token` command and following the
    prompts.

    1.  After you have configured it, you can access the config here to
        check that everything looks good: `~/.databricks-connect`. You might see nulls under cluster_id, org_id, port, please fill them in right in the config if you see that. 

    2.  Run `databricks-connect test` to test your configuration. If you run into errors, first thing to check is that your  `~/.databricks-connect` looks fine.

    3.  refer to this page for a more comprehensive materials on this:
        <https://docs.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect>

16. You need to know values for the following environment variables and
    add the relevant "export" commands at the end of your
    `~/.bash_profile`. Don't forget to re-initialize your bash profile
    using `. ~/.bash_profile` after you add those env vars there.
    
    - To get values for these env vars please reach out to your peers.
    - Env vars you need to set:

        ```
        sas_token
        prod_sas_token
        AZURE_SECRET
        CLIENT_ID
        TENANT_ID
        VAULT_URL
        AZURE_KV
        DIM_PREFIX
        SOBEYS
        SAS_TOKEN
        RUN_ENVT
        AZURE_BLOB_NAME
        AZURE_BLOB_CONTAINER
        ```

# Code Structure 

### "Tasks" (aka "runners"):

-   "Tasks" are top-level \"endpoints\" that are orchestrateable and
    schedulable (on either Airflow or any other tool).

-   view each "task" as a \"building block\" of a pipeline, it should be
    narrowly-focused and serve 1 business purpose.

-   to be explicit about what the task does and to minimize the number
    of ways a task can be called (i.e. to narrow down its purpose) we do
    NOT pass any arguments to the "task\_\' function (other than `spark`
    and `config`, but that will move to imports in future)

-   should not contain any business logic, instead they should call
    "helper" functions that take datasets and parameters and return
    datasets

-   should ONLY live inside `main.py` files and only inside
    `spaceprod/src/<module name>/main.py`

### "Helpers":

-   must be isolated (not accessing data on disk, or context, or
    anything outside of what was passed to it)

-   are helper methods, function or objects that should be defined
    outside of `spaceprod/src/<module name>/main.py` and imported inside
    `spaceprod/src/<module name>/main.py` from
    `spaceprod/src/<module name>/<helper_file_name>.py` to be called at
    the right time with the right parameters

-   take in `SparkDataFrame`\'s and config parameters and return
    datasets

-   easily unit-testable

### Summary tasks vs helpers:
![../img/01_tasks_helpers.png](../img/01_tasks_helpers.png)

### An example of a task structure:
![../img/01_task_structure_example.png](../img/01_task_structure_example.png)


# Using Azure Storage Explorer 

It is a desktop client for accessing and browsing Blob storage
manually.

1.  Download and install Azure storage
    explorer: <https://azure.microsoft.com/en-us/features/storage-explorer/>

2.  open it and click on the \"plug\" sign on the left menu

3.  Select \"blob container\"

4.  Select \"Shared Access Signature (SAS)\"

5.  For display name say \"Sobeys Blob\"

6.  For "Blob container SAS URL" supply a SAS key. You can ask another
    developer for it or create a new one in the Azure portal
    (<http://portal.azure.com> )

7.  Next, Next

# Accessing VM         


Use your AD credentials to SSH to a VM

-   DEV: `ssh your_username@10.166.223.4`

-   PROD: `ssh your_username@10.166.223.132`

Where `your_username` is your AD username.

please note: the above would only work from a Sobeys laptop and Sobeys
network. If you are using a BCG laptop, the hostname would be different
(`52.228.98.186`).

Airflow is running on port 8080, for e.g. to access DEV Airflow,
navigate your browser to `http://10.166.223.4:8080/`

Airflow is running a service. If you ever need to restart it, use:
`sudo systemctl restart airflow-webserver airflow-scheduler`

To view Airflow logs:

``` 
sudo journalctl -u airflow-scheduler -e
sudo journalctl -u airflow-webserver -e
```
