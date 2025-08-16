# Space Productivity Elasticity Curves

## General information
We use a bayesian model to predict the sales or margin per facing for a particular item. The idea is to analyze diminishing return and to then optimize the space with the space optimization.

The bayesian models are partitioned per region, banner, and category, which forms the `Exec_Id`. The overall prior distribution comes from the whole `Exec_Id`, while the posteriors comes in need state and store cluster level.

The resulting prediction is in need state and store cluster level. Therefore all items or stores within will have identical curves.

There was an implementation on more general macro elasticity that predicts in category level, but it was deprecated following phase 2.

## Steps

This module is broken into 5 steps:

1. `task_get_pog_deviations()` : Which cleans up POG files by recalculating theoretical linear space and checking if there is any huge deviation with POG data
1. `task_elasticity_model_pre_processing()` : Which prepares the input data to the bayesian model for generating prediction
1. `task_run_elasticity_model_sales()` : Which creates the prediction bayesian model for sales given facings, store cluster, and need states
1. `task_run_elasticity_model_margins()` : Which creates the prediction bayesian model for margin given facings, store cluster, and need states
1. `task_impute_elasticity_curves()` : Which imputes elasticity curves for items in clusters that did not receive curves during elasticity modeling using cosine distance to centroids

### 1. Getting POG Deviation

Since sometimes the data from POG is not completely clean, we may have to recalculate the total space that each category has based on the information that we have. Based on the information on how much the planogram data deviates from its theoretical value, we filter the model so that it trains only on *"good"* POG data.

To start, take a look at the diagram below for POG terminologies

![](../img/05_planogram_terminology.png)

We are interested in knowing the *Theoretical Linear Space*, which means the total length that a section (or category) is assigned to in a store. We are also interested in knowing the *Linear Space per Break*, which is the total space in length of one *Legal Break* (sometimes refered to as *doors*).

POG deviation is calculated by how much it differs from calculating *Theoretical Linear Space* vs calculating the total width of the items, multiplied by number of facings. Any POG with high deviation is filtered out from model training.

*Theoretical Linear Space* is calculated as follows:

1. Given the *Length Section* (the length of all the doors), and the *Legal Increment Width* (the length of the door), we can figure out the number of *Legal Breaks in Section*
2. Then, we can calculate the *Linear Space per Break* by dividing the total *Width X Facings* with number of *Legal Breaks in Section*
3. Next, we can calculate *No. of Shelves* by dividing *Linear Space per Break* with a range of integer using trial and error to find the best fit
4. Then, *Theoretical Linear Space* is the multiplication of *No. of Shelves* with length of one *Legal Break* and number of *Legal Breaks in Section*

### 2. Pre-Processing

This step focuses on gathering input data and prepare them for the Bayesian Model. A lot of the steps involved are data cleansing and wrangling with the objective of getting information on:

1. Sales and Margins as dependent variables
2. Facings as independent variable
3. Store Cluster and Need States for posterior distribution
4. Region, Banner, and Category for unit of execution and prior distribution

The input data involved are:

1. Need states module output
2. Processed Apollo and Spaceman data from need states module output
3. Merged cluster information (both the automated version, and manual intervention version)
4. Regular ETL outputs, such as:
   1. Store location data
   2. Product hierarchy data
   3. Transaction data
   4. E2E margin data
   5. Calendar data

The model definition are using the following distributions:
$$
y_{pred} \sim N(\beta_{i}\times x,\sigma)
$$
Where *i* is defined as *section*, which is unique index for each store cluster - need state combination within the same `Exec_Id`. Meanwhile, *x* is defined as natural log of number of facings.

Meanwhile, the beta coefficient comes from the following distribution
$$
\beta \sim N(\mu_{\beta}, \sigma_\beta)
$$
Both *sigma* and *sigma beta* are real numbers, with 0 as lower bound. And finally, the model takes the value of *mu_beta* from:
$$
\mu_\beta \sim Cauchy(0,1)
$$
The resulting data is written to blob as input to the elasticity bayesian model.

### 3. Model Creation

This step utilizes the prepared pre-processed data as input to generate a bayesian model to predict sales and margin given number of facings.

This task is wrapped inside UDF to be processed in parallel. The unit of execution is on `Exec_Id`, which is the combination of region, banner, and category. `Exec Id` is also the prior distribution, while the posteriors are calculated per store cluster and need states level.

Sample of the elasticity curve plots are shown here:

![](../img/05_elasticity_curve_samples.png)

This process is repeated twice, each one for sales and margins respectively.

### 4. Imputation

Lastly, any item that doesn't have elasticity curve (assigned to its store cluster - need states), gets its values imputed by finding the closest merged cluster centroids it belongs to. Distance is calculated using cosine similarity.

## Sample Final Output

Final output of the whole process looks like below:

| **Region**   | **National_Banner_Desc** | **Section_Master** | **Item_No** | **Item_Name**               | **Cannib_Id** | **Need_State** | **Cannib_Id_Idx** | **Need_State_Idx** | **Cluster_Need_State** | **Cluster_Need_State_Idx** | **M_Cluster** | **beta**  | **facing_fit_1** | **facing_fit_2** | **facing_fit_3** | **facing_fit_4** | **facing_fit_5** | **facing_fit_6** | **facing_fit_7** | **facing_fit_8** | **facing_fit_9** | **facing_fit_10** | **facing_fit_11** | **facing_fit_12** | **facing_fit_0** | **Margin** | **Sales** | **MERGE_CLUSTER_REFERENCE** | **IS_IMPUTED** |
| ------------ | ------------------------ | ------------------ | ----------- | --------------------------- | ------------- | -------------- | ----------------- | ------------------ | ---------------------- | -------------------------- | ------------- | --------- | ---------------- | ---------------- | ---------------- | ---------------- | ---------------- | ---------------- | ---------------- | ---------------- | ---------------- | ----------------- | ----------------- | ----------------- | ---------------- | ---------- | --------- | --------------------------- | -------------- |
| **atlantic** | SOBEYS                   | Frozen Natural     | 693375      | PizzOggi Pizza Veggitaly    | CANNIB-930    | 3              | 1                 | 1                  | 3_1C                   | 1                          | 1C            | 33.357006 | 23.121315        | 36.646416        | 46.24263         | 53.68603         | 59.76773         | 64.90974         | 69.363945        | 73.29283         | 76.80734         | 79.98661          | 82.889046         | 85.55903          | 0                | 61.671295  | 175.05965 | null                        | 0              |
| **atlantic** | SOBEYS                   | Frozen Natural     | 847791      | Carbnaut Buns Hot Dog       | CANNIB-930    | 2              | 1                 | 2                  | 2_2A                   | 2                          | 2A            | 7.752352  | 5.3735213        | 8.5168295        | 10.747043        | 12.47693         | 13.89035         | 15.0853815       | 16.120564        | 17.033659        | 17.85045         | 18.589329         | 19.263872         | 19.884392         | 0                | 4.396061   | 13.194328 | null                        | 0              |
| **atlantic** | SOBEYS                   | Frozen Natural     | 450677      | GreenOrg Soy Edamame Frozen | CANNIB-930    | 8              | 1                 | 3                  | 8_1C                   | 3                          | 1C            | 11.900806 | 8.24901          | 13.074372        | 16.49802         | 19.153608        | 21.323383        | 23.1579          | 24.747032        | 26.148745        | 27.40262         | 28.536888         | 29.572393         | 30.524965         | 0                | 24.931505  | 70.048134 | null                        | 0              |

It has information on region, banner, category (section master). Items across the same need state and store merged clusters have identical curve, and thus identical prediction values.

*Beta* is the coefficient. Facing fit columns are the model's predicted value (could be sales or margins) and are calculated as:
$$
facing\_fit_i = \beta \times log_e(i)
$$
Where *i* is the predicted number of facings.
