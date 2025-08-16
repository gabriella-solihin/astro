# Generating Customer Need States <br>

### Running the module <br>
ensure that tensorflow==2.5.0 is used for the need states. inno-utils may be conflicting when installing requirements.txt but inno-utils is not needed for this module. 
If 2.5 is not used, a "tuple" error may occur.

### Module Objective <br>

The purpose of this module is to generate customer driven Need States where a Need State is a set of items that are 
considered to be substitutes by a customer.  This is illustrated in the image below where the same customer is 
purchasing 2 different brands of chips but *never together*. <br>

![](../img/04_subs_intro.png)

### Configuration parameters <br>

| Parameter                   | meaning                                                                                                                                                  | recommended value                                                    |
|-----------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------|
| region                      | region to create need states                                                                                                                             | Ontario                                                              |
| trans_path                  | path to raw transaction data                                                                                                                             | prod_outputs/etl/ontario/txnitem                                     |
| prod_path                   | path to product hierarchy data                                                                                                                           | prod_outputs/etl/all/product/latest                                  |
| store_path                  | path to location data                                                                                                                                    | prod_outputs/etl/all/location/latest                                 |
| contactability_flag_path    | path to mapping between CUSTOMER_SK and CUSTOMER_CARD_ID                                                                                                 | sobeys_space_prod/dev/need_states/contact_flag_eda/parsed_flag_data  |
| need_state_save_path        | path to save the need state output                                                                                                                       | sobeys_space_prod/dev/need_states/outputs/                           |
| banner                      | banners to run within the region (takes a list of banners if desired to run more than one                                                                | SOBEYS                                                               |
| st_date                     | start date to pull transactions to generate need states                                                                                                  | N/A                                                                  |
| end_date                    | end date to pull transactions to generate need states                                                                                                    | 52 weeks from st_date                                                |
| category                    | the level 4 category name on which to generate the need states                                                                                           | Ice Cream                                                            |
| num_prods                   | the number of products being used to generate need states                                                                                                | 912                                                                  |
| visit_thresh                | the number of times a customer must have visited the category to be in the modelling                                                                     | 5                                                                    |
| item_vol_thresh             | the number of transactions the item must have been purchased by the same customer in different transactions to be considered - removes low volume items  | 100                                                                  |
| batch_size                  | the batch size used in the neural net when obtaining item embeddings                                                                                     | 32                                                                   |
| embedding_size              | the size of the item embedding                                                                                                                           | 128                                                                  |
| num_ns                      | the number of negative samples to use for each positive sample                                                                                           | 5                                                                    |
| max_num_items               | the maximum number of items an individual customer can purchase in the category                                                                          | 50                                                                   |
| steps_per_epoch             | the number of batches to run per epoch for the neural net                                                                                                | 1000                                                                 |
| num_epochs                  | the number of epochs to run for the neural net                                                                                                           | 100                                                                  |
| valid_window_size           | the number of items to use for validation e.g. randomly choose the top X products for validation                                                         | 20                                                                   |
| valid_size                  | the number of 'similar' items to show for each validation item                                                                                           | 20                                                                   |
| save_path                   | path to save the neural net model                                                                                                                        | sobeys_space_prod/dev/need_states/outputs/                           |
| save_item_embeddings_path   | path to save the final item embeddings                                                                                                                   | N/A                                                                  |
| save_item_embeddings_period | number of epochs to save item embeddings as model fits                                                                                                   | 1                                                                    |
| early_stopping_patience     | number of epochs to run without improvement in evaluation metrics                                                                                        | None                                                                 |
| save_period                 | number of epochs between model saves during training                                                                                                     | 1                                                                    |
| run_similarity_qa           | runs the validation checks showing "most similar" products for a chosen set of items                                                                     | True                                                                 |
| similarity_period           | number of epochs between running validation checks                                                                                                       | 1                                                                    |
| cosine_sim_save_path        | path to save the cosine similarity output                                                                                                                | sobeys_space_prod/dev/need_states/outputs/                           |

<br>

### Data Sources <br>

#### Input Data <br>

*Transaction*

| key column name           | type        | meaning                     | note                                                                                             |
|---------------------------|-------------|-----------------------------|--------------------------------------------------------------------------------------------------|
| CALENDAR_DT               | date        | date for transaction        | key column in filtering preferred window for transactions                                        |
| TRANSACTION_RK            | transaction | key for a transaction       | main key for this table                                                                          |
| ITEM_SK                   | product     | key for an item             | main key for this table                                                                          |
| CUSTOMER_SK               | customer    | key for a customer          | special value of 1 stands for customers who cant be mapped to loyalty program                    |
| CUSTOMER_CARD_SK          | customer    | key for a customer          | maps to CUSTOMER_CARD_ID which is the unique key for a customer that can be mapped over time     |
| RETAIL_OUTLET_LOCATION_SK | location    | key for store               | key for store location                                                                           |
<br>

*Product*

| key column name       | type         | meaning                        | note                                                                                                                                                           |
|-----------------------|--------------|--------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ITEM_SK               | key          | key indicates item             | linkage to txnitem table                                                                                                                                       |
| ITEM_NO               | key          | key indicates item             | more stable ID for item than ITEM_SK, linkage to family_hierarchy table. Equivalent to MASTER_ARTICLE when the ITEM_NO is not found in family_hierarchy table  |
| CATEGORY_ID (LVL4_ID) | level        | 2nd lowest level               | most commonly used in category shopping feature engineering e.g. dry pasta                                                                                     |

<br>

*Location*

| key column name           | type         | meaning               | note                                                                    |
|---------------------------|--------------|-----------------------|-------------------------------------------------------------------------|
| RETAIL_OUTLET_LOCATION_SK | key          | key of each store     | linkage to txnitem table                                                |
| STORE_NO                  | key          | key of each store     | more stable Store ID over time - does not change with franchisee change |
| NATIONAL_BANNER_DESC(CD)  | descriptions | banner name (or code) | used to select certain banner, e.g. Sobeys                              |

<br>

#### Output Data <br>

*Need State Mapping*

Maps items to their final Need States

| key column name           | type         | meaning               | note                                                                                                                                                                          |
|---------------------------|--------------|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| NEED_STATE                | group        | Need State that the item maps to     | represents the need state that the item belongs to                                                                                                             |
| ITEM_NO               | key              | key indicates item                   | more stable ID for item than ITEM_SK, linkage to family_hierarchy table. Equivalent to MASTER_ARTICLE when the ITEM_NO is not found in family_hierarchy table  |
| ITEM_NAME             | descriptions     | description of item                  | surrogate of MASTER_ARTICLE description when ITEM_NO not found in family_hierarchy table                                                                       |

<br>

*Item Similarity*

Provides the Cosine Similarity between all pairs of items


| key column name           | type         | meaning                                   | note                                                                                                                                                                          |
|---------------------------|--------------|-------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ITEM_A                    | key          | key indicates item                        | the item number for the first item in the pair                                                                                                                                |
| ITEM_B                    | key          | key indicates item                        | the item number for the second item in the pair                                                                                                                               |
| ITEM_ENG_DESC_A           | descriptions | description of item                       | the item description for the first item in the pair                                                                                                                           |
| ITEM_ENG_DESC_B           | descriptions | description of item                       | the item description for the second item in the pair                                                                                                                          |
| COSINE_SIMILARITY         | value        | cosine similarity between the item pair   | the cosine similarity based on the item embedding vectors                                                                                                                     |

<br>

### Overview of algorithm <br>

#### Step 1 - Data Prep <br>

For every customer all of their historical purchases over a defined period (usually 52 weeks) are aggregated to provide 
a list of the items that they purchased in *separate transactions* i.e. if a pair of items were ever purchased together 
by a customer then they are not considered as substitution candidates. <br>

![](../img/04_data_prep_aggregates.png)

As part of the data prep model a generator is built that provides pairs of positive and negative samples utilizing 
the Skipgrams function in Keras where a positive pair is a pair of items where the customer did purchase the items 
in separate transactions and a negative pair where they did not.  As the Skipgrams function is being utilized 
negative sampling is used to sample the negative pairs (oversampling more frequently purchased items). <br>

![](../img/04_data_prep_sampling.png)

#### Step 2 - Modelling <br>

Item similarity is measured by passing the input data from the generator through a simple Neural Network as shown
below: <br>

![](../img/04_modelling_architecture.png)

The network takes as input "target" and "context" items, creates embeddings of each, generates the Cosine Similarity
between the embeddings and predicts the supplied label identifying if the target and context items were bought by that
customer in different transactions. <br>

"targets" and "context" are item predictions on whether an item is a substitute for another item.

The key output from the network is the target embeddings that are generated for each item.  <br>

#### Step 3 - Create Need States <br>

Once the embeddings for each item are created, the next step is to understand the similarity of items by calculating
the Cosine Similarity between all pairs of items. <br>

The Cosine Similarity measures the angle between the pairs of embedding vectors and is calculated with the following 
formula: <br>

![](../img/04_need_states_cosine_formula.png)

The illustration below shows an example of how the angle between the embedding vectors represents their similarity: <br>

![](../img/04_need_states_cosine_sim_illustration.png)

To generate the Need States a distance matrix is created that contains the Cosine Similarity between all pairs of 
items (technically 1/cosine similiarity as the clustering in the next step is a minimization problem) as illustrated
below: <br>

![](../img/04_need_states_dist_matrix_example.png)

From the distance matrix a dendrogram is generated using Hierarchical Clustering: <br>

![](../img/04_need_states_dendro.png)

The dendrogram identifies how customers shop the category by exposing the customer decision tree, for example, in 
Frozen Pizza the biggest break in the dendrogram is between thin and thick crust pizzas, then by brand, then by 
flavor. By cutting the dendrogram at a determined cut-point the Need States are generated.  The table below shows some
example Need States for Frozen Pizza: <br>

![](../img/04_need_states_example_need_states.png)

The Need States clearly represent sets of substitutable items that the customer decides between.  <br>
