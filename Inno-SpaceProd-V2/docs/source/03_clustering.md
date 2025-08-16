# Space Productivity Clustering


## 1. input data dictionary

#### 1. competitor data
| key column name           | type        | meaning                     | note                                                                                             |
|---------------------------|-------------|-----------------------------|--------------------------------------------------------------------------------------------------|
| BK                        | Store       | Sobey's store number        | main key for this table                                                                          |
| Drive_Time                | time        | drive time to Sobeys store  | 5, 10, and 15 mins drive time are pulled                                                         |
| COMP_BANNER_KEY           | Store       | Competitor's banner key     | main key for competitor stores                                                                   |
| COMP_BAN_TYPE_DESC        | Store       | Competitor's description    | column includes the name of competitor store                                                     |
| COMP_BANNER_FORMAT        | Store       | Competitor's banner type    | the banner type of competitor store e.g., full service, discount                                 |
| wkly_sales                | sales       | weekly sales                | Sobey's store's weekly sales amount related to groceries                                         |
| tot_wkly_sales            | sales       | total weekly sales          | Sobey's store's total weekly sales amount of all items                                           |


#### 2. demographic data
| key column name                      | type        | meaning                     | note                                                                                  |
|--------------------------------------|-------------|-----------------------------|---------------------------------------------------------------------------------------|
| Banner_Key                           | Store       | Sobey's store number        | main key for this table                                                               |
| Banner_Name                          | Store       | Sobey's store name          | column includes the name of Sobey's store                                             |
| Total Population                     | Demographic | Population by age group     | Population of competitor store's neighborhood                                         |
| Total Visible Minorities             | Demographic | Visible minority group      | 2020 Total Visible Minorities break down by minority group                            |
| Total Immigration  by Place of Birth | Demographic | Immigration group           | 2020 Household Population for Total Immigration  by Place of Birth                    |
| Household Income                     | Demographic | Household Income            | 2020 household income in different ranges                                             |
| Educational Attainment               | Demographic | Education GROUP             | 2020 Household Population 15 Years or  Over for Educational Attainment                |



#### 3. transaction data
| key column name           | type        | meaning                     | note                                                                                             |
|---------------------------|-------------|-----------------------------|--------------------------------------------------------------------------------------------------|
| CALENDAR_DT               | date        | date for transaction        | key column in filtering preferred window for transactions                                        |
| ITEM_SK                   | product     | key for an item             | secondary key for this table                                                                          |
| CUSTOMER_SK               | customer    | key for a customer          | special value of 1 stands for customers who cant be mapped to loyalty program                    |
| ITEM_QTY                  | sales       | quantity sold               | key column used in quantity related feature engineering                                          |
| SELLING_RETAIL_AMT        | sales       | final selling price         | key column used in sales related feature engineering                                             |
| HOST_RETAIL_AMT           | sales       | Pre-promo price             | columns not used in current feature engineering, might be used for price sensitivity calculation |
| FLYER_SK                  | promotion   | key for flyer               | special value of -1 stands for items not in from flyer                                           |
| PHYSICAL_STORE_LOCATION_NO| location    | key for store               | key for store location                                                                           |
| PROMO_SALES_IND_CD        | promotion   | indicates the type of promo | categorical indicating types of promotions                                                       |

#### 4. product
| key column name       | type         | meaning                        | note                                                                                                                                                           |
|-----------------------|--------------|--------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ITEM_SK               | key          | key indicates item             | linkage to txnitem table                                                                                                                                       |
| ITEM_NO               | key          | key indicates item             | more stable ID for item than ITEM_SK, linkage to family_hierarchy table. Equivalent to MASTER_ARTICLE when the ITEM_NO is not found in family_hierarchy table  |
| ITEM_NAME             | descriptions | description of item            | surrogate of MASTER_ARTICLE description when ITEM_NO not found in family_hierarchy table                                                                       |
| LVL5_ID               | level        | 1st lowest level               | e.g. Domestic pasta                                                                                                                                            |
| CATEGORY_ID (LVL4_ID) | level        | 2nd lowest level               | most commonly used in category shopping feature engineering e.g. dry pasta                                                                                     |
| LVL3_ID               | level        | 3rd lowest level               | e.g. meal makers                                                                                                                                               |
| LVL2_ID               | level        | highest level                  | e.g. grocery ambient; corresponding LVL2_NAME is used to filter out items e.g. Lottery, Tobacco                                                                |

#### 5. location
| key column name           | type         | meaning               | note                                       |
|---------------------------|--------------|-----------------------|--------------------------------------------|
| PHYSICAL_STORE_LOCATION_NO| key          | key of each store     | linkage to txnitem table                   |
| NATIONAL_BANNER_DESC(CD)  | descriptions | banner name (or code) | used to select certain banner, e.g. Sobeys |

## 2. external clustering

External clusters are calculated using catchment area demographics & competition e.g., young vs. old, competed vs. un-competed.

The diagram of external clustering pipeline is shown below.

![](../img/03_external_clustering_diagram.png)

The input data consists of competitors and demographic data.  

The data treatment of demographic data includes:

1. Cleaning the dataset from null values and special characters in column names
2. Perform manual corrections on some store numbers due to data issue
3. Calculate Count of demographic groups (e.g. German-born population) within 5, 10, 15 mins drive. Counts on lower drive time is added to the higher drive time (e.g. 15 mins drive count has 5 mins and 10 mins added to it)

The data treatment of competitor data includes:

1. Cleaning the dataset from null values and special characters in column names
2. Perform manual corrections on some store numbers due to data issue
3. Calculate Count of Special Stores (e.g., Costco, Walmart) within 5, 10, 15 mins drive
4. Calculate Count of Banner Types (e.g., Discount, Full Services, Community) within 5, 10, 15 mins drive
5. Calculate Total Area of competitor stores within 5, 10, 15 mins drive. Counts on lower drive time is added to the higher drive time (e.g. 15 mins drive count has 5 mins and 10 mins added to it)

After data preparation, the next step is feature agglomeration, which includes the following steps:

1.  **Preprocessing**: This data step before clustering is performed under a vanilla UDF
    1.  Drop features that has 0 variance across the dataset since it does not contribute to the clustering process
    2.  Perform data Normalization
        We need to normalize the strength (there may be more immigration features from every country than the 10 surrounding competitors that grouped in the comp. feature)
2.  **Perform hierarchical clustering**
        We cluster the geographic and competitor features here.
3.  **Tune the distance** of dendrogram between levels of leaves to select the optimal number of clusters
        This is a manual step where we want to make sure that the distinct groups have meaning to the business (e.g. are not too granular to not be actionable and not too vague to not inform the difference between them)
4.  **Reduce the number of feature candidates** (competitor and geography) by selecting the top n (n=1) features per cluster 
    based on the strongest absolute correlation with sales per square feet

A sample dendrogram produced by feature agglomeration is shown below:
![](../img/03_feature_dendrogram.png)

After the feature is selected, the next step is to cluster Sobey's stores. Using the selected features from feature clustering, 3 clusters of stores were generated.

A sample dendrogram from previous run, produced by store clustering, is shown below:

![](../img/03_store_dendrogram.png)

The output of this module goes to **4. Merged Cluster** to be merged with internal clustering result, and also to the profiling module where we calculate the index of mean and standard deviations of each cluster. The result of profiling module is eventually used as dashboard, while the result of merged cluster is used throughout spaceprod product stream.

## 3. internal clustering

### 3.1 Problem
To classify a group of stores (e.g. all stores within region of _Ontario_
and of _Sobeys_ banner) into certain number of clusters, with stores in
each cluster have similar customer behaviour in terms of shopping
pattern. 

### 3.2 Algorithm
The problem obviously falls into the scope of unsupervised statistical
learning and more specifically the classification. Decision has been
made to go with the scipy _fcluster_ package.
```python
lkd = linkage(proportions_matrix, method="ward")
clusters = fcluster(lkd, num_clusters, criterion="maxclust")
```
Currently the _num_clusters_ is set to be 3 for internal store clustering
and 3 for external store clustering as well, a.k.a _3X3_.

### 3.3 Model training level / parallelization
The current setup is to loop through each _region_ and _banner_ and do
internal store clustering for each combination. The reason for this is
that there are currently only 10 _region_ _banner_ combinations and for
each the clustering runs fairly quick. 

````python
# iterate over each combination of REGION/BANNER
for iter in list_iter:
    region = iter["REGION"]
    banner = iter["BANNER"]
    log.info(f"Clustering for region {region} and banner {banner}")
````

### 3.4 Input data / training features
Features selected for model training are sales proportion for each
need states within a particular POG section(aka category). The form of
the input dataset may look like the table below. Basically, columns are
sales or quantity proportion (or whatever metrics in the future) for
each _category_ _needstate_ combination - these are the training features;
each row is a store within a specific _region_ _banner_. 

| |cat_1|cat_1|cat_1|...|cat_n|cat_n|cat_n|
|---|---|---|---|---|---|---|---|
|store|ns_1|ns_2|ns_n|...|ns_1|ns_2|ns_n|
|111|
|222|
|333|
|444|
|555|

Note on **NeedState:**

Need states are unique customer needs. E.g. a thin crust pepperoni 
pizza is a need state that should always be fulfilled. This need state
may be satisfied with any brand thin crust pepp. so that items are 
substitutable. Once need states for all categories have been created(
this is modelled by the _NeedState module_ by using prod2vec algorithm), 
We query per store what the sales for each category-need-state were. 
This is what we call sales proportions. These sales proportions per 
store category need state are then used in the clustering algorithm to
determine which stores look alike from an internal item sales or margin
perspective. Veggies is a special case because we still relied on the 
cannib veggie category.

### 3.5 outputs
The main output is simply a mapping table which contains store and its cluster
assignment.

The other two outputs are two dataframes, 
i.e. `df_region_banner_store_section_prop_qty` and 
`df_region_banner_store_section_NS_prop_qty`, which contains _prop\_qty_ 
for a store at _section_ level and _section\_ns_ level respectively.
These two dataframes are going to be used in the profiling process
that follows the clustering.

### 3.6 Profiling
Cluster membership(i.e. cluster lables) are appended to the two
dataframes mentioned above. Therefore, for each store, the _prop\_qty_
is at the granularity of _cluster_, _store_, _section_ level and
_cluster_, _store_, _section_, _NeedState_ level respectively. 
_Mean_ and _Stddev_ first are calculated at _cluster_ and _section_
level and _cluster_, _section_ and _NeedState_ level, i.e. aggregate
out the _store_ dimension. And then _Mean_ and _Stddev_ are calculated
at _section_ and _section_, _NeedState_ level respectively, aggregating
out _cluster_. At the end, _index_ of _prop\_qty_ is calculated as:

- _section_ level:
    * _Mean Index_ = (Mean of **cluster** & **section**) / (Mean of **section**)
    * _Stddev Index_ = (Stddev of **cluster** & **section**)/(Stddev of **section** )

- _section & NS_ level:
    * _Mean Index_ = (Mean of **cluster** & **section** & **NS**) / (Mean of **section** & **NS**)
    * _Stddev Index_ = (Stddev of **cluster** & **section** & **NS**)/(Stddev of **section** & **NS**)
    

## 4. Merged Clusters
Merged clusters are the combination of both internal and external clusters. There is no code to do this as it is a process decided with the business. 

E.g. If 5 stores are in internal cluster A but they are broken up into external clusters 1 and 2, it would make sense to create two merged clusters,
one with 2 stores in 1A and 3 in 2A. The clustering code reads in the merged_clusters_file which is manually created.

The diagram of external clustering pipeline is shown below.

![](../img/03_internal_clustering_diagram.png)

An example of frozen pizza dendrogram is shown below:

![](../img/03_frozen_pizza_dendrogram.png)