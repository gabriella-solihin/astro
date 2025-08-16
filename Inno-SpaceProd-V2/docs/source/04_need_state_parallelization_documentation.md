## 0. Planogram processing
Planogram is an critical input to this process, as everything
downstream build upon it. There are currently two sources for it,
i.e. Apollo and Spaceman (both are planogram service providers). Datafeed from both sources are ingested,
deduplicated, cleaned and validated, in order to provide a high
quality input to downstream modelling. 

In the future, these processing here are probably going to be decoupled
from NeedState module and reallocated to ETL pipeline where
it belongs to. For now it's kept here because NeedState module is
its first entry point to the whole pipleline. Also, BY(BlueYonder)'s
service will be integrated into Sobeys's system, thus By is probably
going to be the planogram service provider in the future.

### 0.1 Apollo
`task_pre_process_apollo_data()` is the one that does these processing
described above. Please refer to this [DB notebook](https://adb-5563199756815371.11.azuredatabricks.net/?o=5563199756815371#notebook/84504757361843/command/84504757362101) for more details.

### 0.2 Spaceman
`task_pre_process_spaceman_data()` process spaceman planogram data feed. 
Please refer to this [DB notebook](https://adb-5563199756815371.11.azuredatabricks.net/?o=5563199756815371#notebook/84504757361886/command/4027071744110167)
for more details. 

### 0.3 Concatenate planogram data
Once both planogram data have been processed properly, they are concatenated
together to make the single planogram input. This [DB notebook](https://adb-5563199756815371.11.azuredatabricks.net/?o=5563199756815371#notebook/84504757361929/command/2326599057726793)
details it. 

## 1. Parallelization

Why is Parallelization necessary? There are hundreds of categories and multiple REGION & BANNER to 
run need state clustering for. To run these hundreds of clustering process in sequential order would not be time efficient.
A better way would be to distribute these jobs to dozens or more computational worker nodes to run the job in parallel. The Databricks platform 
provides the technology for distributed computing at Sobeys, therefore, most of Advanced Analytics tasks are parallelized to take advantage
of fast computation technology. 

The granularity of the parallelization is at the level of REGION in the codebase. Within each REGION, 
user is able to run multiple BANNER and POG CATEGORY by configuring properly in the .yaml file. Parallelization is achieved
through UDFs. Imagine a dataframe, each BANNER and CATEGORY is in a row, the columns contain the inputs for need states clustering, then
UDFs are called on each row with the input columns as parameters, and output as new columns to the 
dataframe. In this sense, all rows (region & category combination) are processed by UDFs in parallel.

The parallelization changes have been implemented in the refactor/ns_creation branch and submitted in this [PR](https://dev.azure.com/SobeysInc/Inno-SpaceProd/_git/Inno-SpaceProd/pullrequest/21359). The overall code organization has changed compared with POC phase 1 documentation mentioned earlier above. Now the need states creation module structure is as below. `main.py` are the "runners" which then call functions that are defined in `helpers` or other scripts in the folder. 

| sub module      | scripts in the sub-module                        |
| :-------------- | ------------------------------------------------ |
| pre_processing  | main.py<br>helpers.py                            |
| prod2vec        | main.py<br>helper.py<br>udf.py<br>udf_helpers.py |
| post_processing | main.py<br>udf.py<br>helpers.py                  |



### 1.1 pre_processing

`Pre_processing` sub-module preprocesses all the need state creation data including transactions, POG sections, and date filtering.  The inputs and output are displayed in the following table. The data manipulations in this sub-module is very straightforward and has already been [documented previously](https://sobeysdigital.atlassian.net/wiki/spaces/INNO/pages/5363073394/Need+State+Creation+and+Clustering). Therefore, we won't go into details of it.

| inputs                                                       | output                                                       |
| ------------------------------------------------------------ | ------------------------------------------------------------ |
| transactions<br>product_hierarchy<br>store_hierarchy<br>contractibility<br>POG_sections<br>POG section master | df_cust_item_diff (columns: customer_card_id, item_no, section_master):<br>dataframe at the customer level that contains all items purchased by that customer in DIFFERENT transactions<br> |

 The `get_core_trans()` function pulls sales data from transaction,
with product hierarchy and store information appended to it. Further 
filters are applied to it, to exclude customers who didn't shop frequently,
exclude items that were not purchased frequently, etc. 
 
`get_items_diff_trans()` is the main function which generates substitutible
product pairs. Substitutible pairs are defined as (different) items that were purchased
by the same customer in different basket(as identified by transaction_sk).
It outputs essentially for each customer the product pairs (item_a, item_b)
purchased in the same POG section, in a particular region, banner.

`get_items_lost_after_explosion()` keeps track of items that are filtered
out in the above process where substitutible pairs are created. More
specifically, following filters exclude quite a bit of items.
```python
Calculates a summary of items that were lost during the explosion
where we filter for:
    BASK_KEY_A!=BASK_KEY_B
    ITEM_A!=ITEM_B
```

### 1.2 prod2vec

`task_fit_prod2vec` is the runner function of this `prod2vec` sub-module which fits the actual products to vector model for the items in a POG category. The detailed process and algorithm have been already explained in the [document](https://sobeysdigital.atlassian.net/wiki/spaces/INNO/pages/5363073394/Need+State+Creation+and+Clustering) mentioned earlier. Refer to it for details. We'll document only what have changed since then.
#### 1.2.0 Exec_ID
`generate_execution_id()` takes the output of `get_items_diff_trans` and
generate execution_id which is essentially a unique combination of `region`,
`banner` and `section_master`. Each ID represents a single model run, and 
all these runs are parallelized by Pandas UDF.

#### 1.2.1 df_lists (`use_pairs` = (`False`, or `True`))
The dataframe `df_cust_item_diff` created above contains all items pairs 
purchased by every customer in different transactions, for all section masters 
specified in the run. If the `use_pairs` parameter is set to `True` (this
is the default at the moment), then these pairs are used directly as positive
pairs in the following Prod2vec modelling. This logic is added during POC 
phase 2, as an 'correction' to how it's implemented during POC phase 1, which
is what happens if `use_pairs = False`. 

Then `limit_modeling_scope` excludes POG sections that are not eligible
for modelling due to various reasons, but mainly two. Check the code 
base for details.

It's taken as input by function `create_prod_lists()` 
to generate 3 dataframes which are then concatenated together. 

`df_item_list` is at section master level, as below. For each section master, the `ITEM_NO_LIST`contains all the items purchased by all customers during the specified time frame. The items in the list are **not** unique. The occurences of an item indicates the number of times it's been purchased. 


| SECTION_MASTER       | ITEM_NO_LIST         |
| -------------------- | -------------------- |
| Frozen Juice         | [452185, 283021, ... |
| Frozen Vegetables    | [250372, 290785, ... |
| Frozen Natural En... | [450662, 450662, ... |
| Frozen Handhelds     | [892065, 679949, ... |
| Frozen Breakfast     | [190405, 375596, ... |

`df_item_cust_set`is at section master level, as below. For each section master, the `CUST_ITEM_NO_SET_MAP`contains a map from `customer` to the `set` of `items` purchased by the customer. Here the items in the set are unique, as opposed to above. 

|SECTION_MASTER|CUST_ITEM_NO_SET_MAP|
|---|---|
|        Frozen Juice|[8000018997 -> [4...|
|        Frozen Pizza|[8000007909 -> [1...|
|       Frozen Potato|[8000006639 -> [4...|
|      Frozen Dessert|[8000029577 -> [8...|
|    Frozen Breakfast|[8000008669 -> [8...|

`df_item_lookup_dict` is also at section master level, as below. For each section master, the `ITEM_LOOKUP_DICT` contains the map from `item` to `item_name`. The `item` are unique because it's simply a lookup table.

|SECTION_MASTER| ITEM_LOOKUP_DICT |
|---|---|
|  Frozen Pizza|[679000 -> DrOetk...|

`ITEM_NO_LIST` is as below.

```[
[
	679000 -> DrOetker Ristrnt Pizza Margher, 
	554798 -> Delissio CrspyPan Pza Mt Lvrs, 
	405922 -> DrOetker Rist T/Crst Pza Spin, 
	956188 -> DrOetker Ristorante Pizza Cavo, 
	576781 -> DrOetker Giusep ThnCrst Parisn
	...
]	
```

Since all 3 dataframes are at the level of section master, they are then joined together to create a dataframe which is `df_list`which basically contains for each section master, `ITEM_NO_LIST`, `CUST_ITEM_NO_SET_MAP` and `ITEM_LOOKUP_DICT`. 

#### 1.2.2 df_prod2vec

`df_list` created above is taken as input by function `create_data_for_prod2vec_by_category()` to dataframes to be used by the  prod2vec TF model. The original logic that's used to generate these dataframes, as below, is reserved and wrapped by the UDF `input_creation_udf` . Refer to `create_data_for_prod2vec` for original logic. 

| input   | outputs                                                      |
| ------- | ------------------------------------------------------------ |
| df_list | all_basket_data<br>dictionary<br>reversed_dictionary<br>count |

What's new here is the UDF, defined as below:

```python
col = F.udf(
        lambda item_list, item_group_list, num_prods: input_creation_udf(
            item_list, item_group_list, num_prods
        ),
        schema_ouput,
    )
```

When called in the following way, it then takes as inputs the 2 columns in `df_list`, i.e. `ITEM_LIST`  and `ITEM_GROUP_LIST`, and generates a new column `model_input`.

```python
df = df_lists.withColumn("model_input", col(*cols_to_pass))
```

 This `model_input` colmns actually contains 4 objects which are unpacked as below:

```python
# split up the columns into individual ones
    df = df.withColumn("all_basket_data", F.col("model_input")["all_basket_data"])
    df = df.withColumn("count", F.col("model_input")["count"])
    df = df.withColumn("dictionary", F.col("model_input")["dictionary"])
    df = df.withColumn(
        "reversed_dictionary", F.col("model_input")["reversed_dictionary"]
    )

cols_to_keep = [
        F.col("SECTION_MASTER").alias("cat_name"),
        F.col("ITEM_LOOKUP_DICT"),
        F.col("all_basket_data"),
        F.col("count"),
        F.col("dictionary"),
        F.col("reversed_dictionary"),
    ]

df = df.select(*cols_to_keep)
```

The final output `df` looks like below, which only shows result for Frozen Pizza category. 

| cat_name     | ITEM_LOOKUP_DICT     | all_basket_data      | count                | dictionary           | reversed_dictionary  |
| ------------ | -------------------- | -------------------- | -------------------- | -------------------- | -------------------- |
| Frozen Pizza | [679000 -> DrOetk... | [[33], [5, 79], [... | [[405922, 6454], ... | [754762 -> 76, 16... | [0 -> UNK, 1 -> 4... |

Then, a series of data processing are applied to the `df`:

1. `add_single_value_columns()` adds prod2vec parameters defined in .yaml configuration file to the `df`. 
2. `add_sales_metric_to_input_data()` adds the sales data to each category to be able to tell how "big" in terms of sales magnitude each model run is. This can be used by downstream to apply business rules or for decision making.
3. `limit_modeling_scope()` as it name suggests, excludes categories that are not sepcified to run in the .yaml config.



At the end of these processing, `df` looks like below for Frozen Pizza category. 

| column                     | value                | added by function                  |
| -------------------------- | -------------------- | ---------------------------------- |
| cat_name                   | Frozen Pizza         |                                    |
| ITEM_LOOKUP_DICT           | [679000 -> DrOetk... |                                    |
| all_basket_data            | [[33], [5, 79], [... |                                    |
| count                      | [[405922, 6454], ... |                                    |
| dictionary                 | [754762 -> 76, 16... |                                    |
| reversed_dictionary        | [0 -> UNK, 1 -> 4... |                                    |
| batch_size                 | 32                   | `add_single_value_columns()`       |
| window_size                | 5                    | `add_single_value_columns()`       |
| num_ns                     | 5                    | `add_single_value_columns()`       |
| max_num_items              | 50                   | `add_single_value_columns()`       |
| embedding_size             | 128                  | `add_single_value_columns()`       |
| item_embeddings_layer_name | p2v_embedding        | `add_single_value_columns()`       |
| num_epochs                 | 25                   | `add_single_value_columns()`       |
| steps_per_epoch            | 100                  | `add_single_value_columns()`       |
| early_stopping_patience    | 100                  | `add_single_value_columns()`       |
| item_count                 | 84                   | `add_single_value_columns()`       |
| SELLING_RETAIL_AMT         | 2716843.4898822308   | `add_sales_metric_to_input_data()` |
| IS_RUN_EXCLUDED_THRESHOLD  | 0                    | `limit_modeling_scope()`           |



#### 1.2.3 run_modeling

`run_modeling()` function calls the modelling UDF on the input dataframe and adds the `model_output` column containing embeddings.  There are 2 things new here. 

1. **Parallelization** is implemented by defining a UDF `fit_prod2vec_udf()` which essentially wraps original prod2vec TF modeling logic. 

   ```python
   lamb_func = lambda cat_name, item_count, all_basket_data, dictionary, reversed_dictionary, item_lookup_dict, batch_size, window_size, num_ns, max_num_items, embedding_size, item_embeddings_layer_name, num_epochs, steps_per_epoch, early_stopping_patience: fit_prod2vec_udf_wrapper(
           cat_name=cat_name,
           item_count=item_count,
           all_basket_data=all_basket_data,
           dictionary=dictionary,
           reversed_dictionary=reversed_dictionary,
           item_lookup_dict=item_lookup_dict,
           batch_size=batch_size,
           window_size=window_size,
           num_ns=num_ns,
           max_num_items=max_num_items,
           embedding_size=embedding_size,
           item_embeddings_layer_name=item_embeddings_layer_name,
           num_epochs=num_epochs,
           steps_per_epoch=steps_per_epoch,
           early_stopping_patience=early_stopping_patience,
       )
   ```

   

2. **Error signaling** is implemented in `fit_prod2vec_udf_wrapper()` to signal "time out" kind of error and to help collect the stack trace and errors to provide most informative errors from each worker.


   ```python
   import signal

   def timeout_handler(a, b):
       raise Exception(f"Model train timeout reached! ({timeout_min} minutes)")
   
   signal.signal(signal.SIGALRM, timeout_handler)
   
   signal.alarm(timeout_min * 60)
   
   try:
   
       dict_model_output = fit_prod2vec_udf(*args, **kwargs)
   
       signal.alarm(0)
   
       return dict_model_output
   
   except Exception as ex:
   
       # in case if it breaks collect the stack trace and errors to
       # provide most informative errors from each worker
       tb = traceback.format_exc()
   
       dict_msg = prepare_exception_messages_for_output(
           msg_exception=str(ex), msg_traceback=str(tb)
       )
   
       signal.alarm(0)
   ```

   

The UDF is called in the following way and the output is saved as `model_output`

```python
# kick off modelling by calling the UDF and storing results in
    # 'model_output' column
    udf_func = F.udf(lamb_func, return_type)
    col_model_output = udf_func(*cols_to_pass)
    df_to_run = df_to_run.withColumn("model_output", col_model_output)
```



### 2.3 Post processing

#### 2.3.1 cosine similarity

`calc_cosine_sim()` generates cosine similarity between items given a dataframe with an ITEM_NO and embedding vector. All pairs of ITEM_NO are created by cross-joining the same dataframe. Then a UDF is defined to calculate the dot product of the L2 normal as below:

```python
dot_udf = F.udf(
        lambda x, y: float(np.dot(x, y) / (np.linalg.norm(x) * np.linalg.norm(y)))
    )

col = dot_udf("vector_a", "vector_b").cast(T.FloatType())
```

which is then called on the ITEM_NO pairs dataframe:

```python
cosine_out = all_pairs.withColumn("COSINE_SIMILARITY", col)
```

#### 1.3.2 negative cosine similarity

`replace_negative_cosine_similarity()` as it name suggests, replaces negative cosine similarity with 0 in current state of code. There has been debate over whether it makes more sense not to replace negative with 0, keeping these negatives. TODO: run NS with/without replacing and compare the result, then make decision on this.

Another thing to note is that cosine similarity is converted to distance(dissimilarity) between pairs of items in this way:`distance = 1 - cosine similarity`. Previously it's `distance = 1/cosine similarity` which results in extreme large distance (when cosine similarity is very tiny, e.g. 0.00001). These extreme large distance may dominate / skew distance matrix.

#### 1.3.3 need state creation

##### 1.3.3.1 reshape data

`reshape_data_for_udf()` aggregates item-level data to POG-level data and collapes the item-level data into lists of maps to feed into the UDF for processing.

At first, the df_cosine_sim looks like below. 

| ITEM_B | ITEM_A | COSINE_SIMILARITY | ITEM_ENG_DESC_A      | ITEM_ENG_DESC_B      | CAT_NAME     | COSINE_SIMILARITY_MIN |
| ------ | ------ | ----------------- | -------------------- | -------------------- | ------------ | --------------------- |
| 955952 | 375007 | 0.36777157        | Delissio Pzria 3 ... | Delissio Pizza St... | Frozen Pizza | 0.63222843            |
| 375007 | 955952 | 0.36777157        | Delissio Pizza St... | Delissio Pzria 3 ... | Frozen Pizza | 0.63222843            |

the following aggregation "collapes" data into category level.

```python
col_json = F.create_map(list(chain(*((F.lit(x), F.col(x)) for x in cols_to_pass))))
agg = [F.collect_list(col_json).alias("ITEM_COSINE_DATA")]
df_inputs = df_cosine_sim.groupBy("CAT_NAME").agg(*agg)
```

As you see, the `ITEM_COSINE_DATA` column contains list of map from key(column name) to value(column value), e.g. ITEM-A -> 375007, etc.

| CAT_NAME     | ITEM_COSINE_DATA     |
| ------------ | -------------------- |
| Frozen Pizza | [[ITEM_A -> 37500... |

##### 1.3.3.2 UDF

`generate_need_states_all_pogs()` generates need states output for all POGs using a UDF which is defined as below:

```python
lamb_fun = lambda list_cosine_sim, list_item_desc, min_cluster_size, deep_split: generate_need_states_udf(
        list_cosine_sim, list_item_desc, min_cluster_size, deep_split
    )
```

Input to this function, e.g. `list_cosine_sim`, is generated by "unpacking" the ITEM_COSINE_DATA created above into a Pandas dataframe. This "packing"->"unpacking" manipulation is to enable running need state creation UDF on category level. 

```python
# pre-process inputs to UDF to covert to PDFs
pdf_cos_sim = pd.DataFrame.from_records(list_cosine_sim)
```

##### 1.3.3.3 cut tree dynamically

Once the dendrogram clustering tree is formed, a new approach `cutreehybrid` (a Python translation of the `CuTreeDynamic` R package) is utilized to cut the clustering tree dynamically based on tree branch shape. 

```python
clusters = cutreeHybrid(
        link=linkage_matrix,
        minClusterSize=min_cluster_size,
        distM=condensed_matrix,
        deepSplit=deep_split,
    )
```

##### 1.3.3.4 Apply UDF

The UDF created above is called on each row (each POG category) and generates `model_output_data` which is saved as a column.

```python
col = F.udf(lamb_fun, schema_output)(*cols_to_pass)
df = df.withColumn("MODEL_OUTPUT_DATA", col)
```
