Prompt

# 1. Overall Requirement


## 1.1 Background
In our company, we are using ab initio (https://www.abinitio.com/en/) as our data processing platform. However, we are looking to transition to Apache Airflow 3 (https://airflow.apache.org/) for better scalability, flexibility, and community support.

Strictly saying, we want to use Airflow 3 to replace Ab Initio Control Center to orchestrate our data pipelines.

## 1.2 Whole workflow

### 1.2.1 developers can upload DAGs or DAGs zip file to compnay insternal nexus repository, and then the DAGs will be automatically deployed to Airflow 3 landing zone with a pipeline. 


### 1.2.2 Once DAGs are in landing zone, there's pre-processing, like code quality check and syntax check, DAG tag assignment based on TAG source, after that, the DAGs will be automatically deployed to Airflow 3 dags folder.


### 1.2.3 




