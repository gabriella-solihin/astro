# Intro: Space Productivity

Welcome!

## 1. Documentation Structure
This documentation has the following sections that live under `spaceprod/docs/source` in the code repository:

 - [Introduction](/00_intro.md): overview of how code and documentation are structured
 - [Environment Setup](/01_environment_setup_and_coding_standards.md): instructions on setting up the environment
 - [Accessing Data, Config and Run COntext](/02_accessing_data_config_and_context.md): learn how to access data, configuration through the context object.
 - [Main Process Diagram](/02_diagram_simple.md): a visual flow chart that outlines components of the SpaceProd data pipeline.
 - [Clustering module](/03_clustering.md): clustering the stores


**How to view the documentation:**<br>
To view this documentation in Azure Devops, navigate to `spaceprod/docs/source`: https://dev.azure.com/SobeysInc/_git/Spaceprod

To view this documentation in a Sphinx site, assuming the remote server mapped to your local port `30000` you can navigate to: `http://127.0.0.1:30000/`

**Note:**<br>
this documentation contains a number of code snippets / examples and explanations of the
logic related to various components of the pipeline.
We aim to keep them up-to-date, but we let the code speak for itself and we try to **make the code
as self-documenting as possible**. Please use PyCharm IDE features (Ctrl+b) to explore the code
and understand the dependencies between its components and configuration parameters. This documentation
is meant for the **initial introduction and guidance** only.

## 2. Overview of Code and Data Dependencies
You can access the main diagram of data and script dependencies using the below link.

<a href="/_static/diagram.html" target="_blank">Click here to view full diagram</a> (will only work when viewing on a Sphinx Web Page)
![diagram](img/diagram.png)
<a href="/_static/diagram.html" target="_blank">Click here to view full diagram</a> (will only work when viewing on a Sphinx Web Page)

## 3. Repository Structure

Any framework or model logic that is related to Space Productivity lives under `spaceprod` directory.
The `SpaceProd` directory is organised into the following sub-folders:

- `docs`: source files for the Space Productivity documentation (markdown and image files)

- `spaceprod/src`: the main business logic, i.e. the model logic, ETL, etc. Every folder in this directory corresponds to a seperate task that can run on Airflow.
    
- `spaceprod/utils`: a set of common helper functions 



Lastly, we have the handover checklist here
`https://sobeysdigital.atlassian.net/l/c/YwJjmhXQ`
