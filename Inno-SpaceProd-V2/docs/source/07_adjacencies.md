# Space Productivity Adjacencies Module

## Overview

The macro adjacency module optimizes the ordering of categories (section masters) within a department , on the region-banner level.

This problem is solved using a traveling salesman problem (TSP), in which each section master is a node, and the ‘similarity between sections’ is the distance between the nodes. The association score is calculated using based on an  ‘affinity’ between 2 section masters based on the observed transactions that they have relative to the expected number of transactions should they be independent.

For example, if we observe that frozen pizzas and ice cream are often bought together, then their association scores will be large. We then take the inverse (1 divided by the affinity ratio) to determine the distance between two nodes, since 2 similar sections will be closer together in our TSP graph.

There are pending discussions on potentially changing the TSP method to hierarchical clustering of categories to allow more customizability. However, as per the writing of this documentation, no final decision has been made.

For this page, we will use examples from optimizing one department, Frozen, across region banners. There are ~13 section masters within Frozen. 

## Dependencies

This module utilizes 4 data sets:

- Transactions table
- Product table
- Location table
- Combined (processed) POG data

Only the combined POG data is generated from the pipeline, at the start of need states creation. Thus, the adjacencies module could run near the beginning of the pipeline.

At the same time, currently no modules depend on the outputs of adjacencies. As a result, this module can also run near the end of the pipeline.

## Part 1. Calculating Association Distances

There are 2 sub-modules in adjacencies: calculating association distances, then running adjacency optimization. In the first part, most of the parallelization comes from leveraging spark by increasing the dimensionality of data processing to calculate distances across region-banner-departments.

### 1.1 Pull core transactions

We join the transactions table w/ the location, POG, and product tables to get essential fields like section master, region/banner, etc,.

Output: a row for each section in every transaction. 

![](../img/07_adjacencies_core_transaction.png)

### 1.2 Get all counts of category-pair occurrences within each transaction

We now want to count the number of transactions that featured both categories in a category-pair. 

Output: a row for each category pair, on the region-banner-department level, with the number of observed transactions that featured both categories in the pair.

![](../img/07_adjacencies_core_cat_pair.png)

### 1.3 Get transaction counts of each category

In addition to the counts of category pairs, we also want the number of transactions for each category alone. This is used in 1.5 to calculate the expected number of occurrences for a pair of items.

Output: a row for each category, on the region-banner-department level, with the number of observed transactions that had that category.

![](../img/07_adjacencies_core_trans_count.png)

### 1.4 Join the tables from parts 1.3 and 1.2

![](../img/07_adjacencies_joined_table.png)

### 1.5 Calculate the expected probability of occurrences for each item pair

The expected probability of occurrences for an item pair assumes that the two sections are independent. Thus:

![](../img/07_adjacencies_chi_formula.png)

Where total transactions is the total number of transactions observed in the entire region banner department.

Output: a row for each category-pair, on the region-banner-department level, with the expected probability of transactions involving the pair of sections. 

![](../img/07_adjacencies_chi_sample_table.png)

### 1.6 Calculate the category association distances

The category association is the ratio between the observed probability and expected probability, divided by the expected count. Notably, a larger association means a greater affinity. Hence, we take the inverse (1 divided by the association) to get the distance, since we want nodes with greater affinity to be closer together. This new column is reflected in `distance`

![](../img/07_adjacencies_distances.png)

## Part 2. Running Adjacency Optimization

In part 2, we prepare the data then feed it through a TSP solver. Most of the parallelization are carried out through window functions. In addition, the TSP solver, which runs in python on a region-banner-department, is wrapped in a UDF. 

### 2.1 Get all category pairs

While the output of 1.6 outputs all observed category pairs, this does not necessarily include all possible pairs; if no transaction contained a certain pair of categories, then it would not appear in 1.6. For pairs that don’t exist, their distances are filled with the max distance within the region-banner-department.

![](../img/07_adjacencies_optim_all_pairs.png)

### 2.2 Create an index for each section

Our TSP solver refers to nodes by indices. As a result, we must create a lookup table that maps indices to our section names. This will be used to encode and decode our outputs from the solver later.

**index_lookup**

![](../img/07_adjacencies_index_lookup.png)

**distance_index**

![](../img/07_adjacencies_distance_index.png)

### 2.3 Prepare the TSP input

We create an input data format that represents all of our adjacencies in the graph.

Output: a row for each region-banner-department, with the column TSP_TUPLES containing all the distances between nodes as a list of lists.

Here, the first entry of TSP_TUPLES means that the distance between node 7 and node 6 is 2.903.

![](../img/07_adjacencies_tsp_input.png)

### 2.4 Determine the number of sections in each region-banner-department

The number of sections in also needed for each TSP solve.

Output: a row for each region-banner-department, with the column NUM_CATS containing the number of sections in the region-banner-department.

![](../img/07_adjacencies_num_cats.png)

### 2.5 Run the TSP algorithm

We run the TSP algorithm using a UDF call for each region-banner-department. The current algorithm is calculated using Dynamic Programming (see the [Held-Karp algorithm](https://en.wikipedia.org/wiki/Held–Karp_algorithm) for more details). 

Output: a row for each region-banner-department, with the column OPTIMAL_ADJ being the optimal path, and BEST_DIST being the associated distance when taking the optimal path.

![](../img/07_adjacencies_tsp_algorithm.png)

### 2.6 Get the optimal path descriptions

We now need to lookup the section names from the indices of the optimal path. In addition, we melt the dataframe to present the order of the optimal path using the ORDER column.

![](../img/07_adjacencies_optim_path_desc.png)

### 2.7 Add distance metrics for the optimal path

For each region banner, the dataframe from 2.6 becomes sorted based on the order of traversal. Then, we add the distance between each node, as well as the cumulative distance. 

![](../img/07_adjacencies_optim_distance.png)