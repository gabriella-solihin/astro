
# What is Space Prod "`context`"

`context` object is an object that is shared across all module runs
within a run to provide access both, the config as well as the interface
to reading and writing data to paths pre-configured in the config.

To access context:

```python
from spaceprod.utils.space_context import context
```

NOTE:

-   you no longer need to instantiate config or read YAML files. All of
    this happens automatically on import.

-   also, on import, the config will be automatically validated to
    ensure there are no errors (e.g. no 2 datasets share the same path
    or same key, etc). If there is any issue an Exception will be thrown
    with an informative and actionable error message, please pay
    attention to those.

# Accessing configuration

For e.g. to access Clustering config:

```python
from spaceprod.utils.space_context import context

config_clustering = context.config["clustering"]
config_internal_clustering = config_clustering["internal_clustering_config"]
config_external_clustering = config_clustering["external_clustering_config"]
```

etc.

`clustering` key and other keys in the above example come from
file/folder names. See screenshot below for explanation:

![02_context_config_access.png](../img/02_context_config_access.png)

# Accessing data 

To read data:

```python
from spaceprod.utils.space_context import context

df = context.data.read("my_dataset_id")
```

where `my_dataset_id` is the key from the YML config where this dataset
is defined. Context will find the right dataset automatically across ALL
config files

To write data (assuming you have `df` defined as a `SparkDataFrame` :

```python
from spaceprod.utils.space_context import context

context.data.write(
  dataset_id="my_dataset_id",
  df=df,
)
```

The above will automatically find the path of `my_dataset_id` and will
write data there.

NOTE:

-   By default, the interface will not let you write data that is empty.

-   Interface will not let you write data to external paths.

To learn what is the fully resolved (versioned) path of any dataset:\

```python
from spaceprod.utils.space_context import context

path = context.data.path("my_dataset_id")

print(path)
```

# Run folder 

A run folder is automatically created when you create a new session of
`context` (import it) and start using it to write data. It is needed to
be able to version runs and data that they produce.

Run folder is used to store all the relevant output of the current run.

To learn what your run folder is:

```python
from spaceprod.utils.space_context import context

path_run_folder = context.run_folder
```

Typically it should be `dbfs:/mnt/blob/sobeys_space_prod/<YOUR RUN ID>`

The run ID alone can be accessed from `context.run_id`

Saving / resuming runs 

This is a useful feature to continue runs that have been interrupted
(e.g. debugging).

### Saving a run 

To be able to resume a run that happened in past, the session had to be
dumped during that run.

Session is dumped like this: `context.dump_session()`

This will automatically serialize all the information (config + run id)
and store it inside your run folder. Please make sure to do this at the
beginning of each run, otherwise the run cannot be resumed later.

When you call `context.dump_session()` you will see a message printed:
`Dumping session here: dbfs:/mnt/blob/sobeys_space_prod/<YOUR RUN_ID>/session`

### Resuming a run 

To a resume a run, simply call:
`context.reload_context_from_run_folder("<FULL PATH TO THE RUN FOLDER OF THE OLD RUN>")`
.

For example:
`context.reload_context_from_run_folder("dbfs:/mnt/blob/sobeys_space_prod/space_run_20211113_205901_976284")`

You will get a new "welcome" message and you will notice that:

-   your run ID has changed to the run ID of the run you just loaded
    (`context.run_id`).

-   your config has changed to the version of the config that was there
    at the time when the old run was dumped (saved) (`context.config`).

-   your run folder has changed to the run folder of the run you just
    loaded (`context.run_folder`).

# Multiplying / cloning runs 

Sometimes you might want to "multiply" your run into more than 1 run.

**Scenario:**

-   You have a run that completed Need States + Clustering (run ID =
    `space_run_123`)

-   You now want to use this run to do multiple runs of optimization
    each of which will have different configuration. Lets say you need
    to run optimization 3 times with 3 different configurations.

**What you can do is:**

-   Clone your current run (`space_run_123`) into 3 runs. New run IDs
    will be created: `space_run_123_version_a`,
    `space_run_123_version_b`, `space_run_123_version_c`.

**How to clone a run:**

1.  Start a run normally, i.e. either instantiate a new context or load
    an existing one from a run folder (see earlier sections of this
    document on this). Just make sure your `context` object bears the
    run that you want to clone.

2.  Do: `context.clone_run(new_run_suffix="version_a")` ← this will
    create a new run ID by taking your original run ID and appending the
    `new_run_suffix` to the end of it, it will also create a new run
    folder and copy over the data from the original run folder.

    1.  *[IMPORTANT]{.ul}*: this will overwrite your current run ID of
        your `context` object, i.e. `context.run_id` will now say
        `space_run_123_version_a` and not `space_run_123` as before the
        clone operation

3.  To create `space_run_123_version_b` and `space_run_123_version_c`
    repeat steps 1. and 2. above with new suffixes, i.e. could be
    something like:

    ```python
    from spaceprod.utils.space_context import context
    
    # reload again to get the original ID:
    context.reload_context_from_run_folder("dbfs:/mnt/blob/sobeys_space_prod/space_run_123")

    # clone to version B:
    context.clone_run(new_run_suffix="version_b")

    print(context.run_id)
    print(context.folder)
    # this will be:
    # space_run_123_version_b
    # dbfs:/mnt/blob/sobeys_space_prod/space_run_123_version_b

    # reload again to get the original ID:
    context.reload_context_from_run_folder("dbfs:/mnt/blob/sobeys_space_prod/space_run_123")

    # clone to version C:
    context.clone_run(new_run_suffix="version_c")

    print(context.run_id)
    print(context.folder)
    # this will be:
    # space_run_123_version_c
    # dbfs:/mnt/blob/sobeys_space_prod/space_run_123_version_c
    ```

**What it does behind the scenes:**

-   creates a new run ID by taking your original run ID and appending
    the `new_run_suffix` to the end of it.

-   creates a new run folder that is named accordingly.

-   re-dumps the original session and config information with the
    newly-generated run ID into the new run folder.

-   copies over the data from the original run folder to the new run
    folder.

# Diagram of how context info / config flows depending on context action 

![02_context_actions.png](../img/02_context_actions.png)