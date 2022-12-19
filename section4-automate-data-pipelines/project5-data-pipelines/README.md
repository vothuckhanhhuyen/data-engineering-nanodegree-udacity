# Project: Data Pipeline with Apache Airflow
This project continues working on the music streaming company’s data infrastructure by creating and automating/scheduling a set of **data pipelines**. We configure and schedule data pipelines with **Apache Airflow** to extract raw datasets from **S3** to **Redshift**, transform and load data from staging tables to dimensional tables. We also monitor production pipelines by run data quality checks to track data linage.

## Project Structure

```
Data Pipeline with Apache Airflow
|
|____dags
| |____ create_tables_dag.py   # DAG for creating tables on Redshift
| |____ create_tables.sql      # SQL CREATE queries
| |____ sparkify_dag.py        # Main DAG for this ETL data pipeline
| |____ subdag.py              # Subdag for loading dimension tables
|
|____images
| |____ sparkify-dag.png   
|
|____plugins
| |____ __init__.py
| |
| |____operators
| | |____ __init__.py          # Define operators and helpers
| | |____ stage_redshift.py    # COPY data from S3 to Redshift
| | |____ load_fact.py         # Execute INSERT query into fact table
| | |____ load_dimension.py    # Execute INSERT queries into dimension tables
| | |____ data_quality.py      # Data quality check after pipeline execution
| |
| |____helpers
| | |____ __init__.py
| | |____ sql_queries.py       # SQL queries for building up dimensional tables
|
|____docker-compose.yaml
|____README.md
```

## How to Run
1. Create an IAM User in AWS
2. Create a redshift cluster in AWS.
3. Connect Airflow and AWS
4. Connect Airflow to the AWS Redshift Cluster
5. Turn on Airflow by running ```opt/airflow/start.sh```
6. Run create_tables_dag DAG to create tables on Redshift
7. Run sparkify_dag DAG to trigger ETL data pipeline


## Airflow Data Pipeline

### Airflow DAG overview
![dag_graph](images/sparkify-dag.png)

### Operators

**1. Stage Operator**
- The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.
- The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

**2. Fact and Dimension Operators**
- With dimension and fact operators, you can utilize the provided SQL helper class to run data transformations. Most of the logic is within the SQL transformations and the operator is expected to take as input a SQL statement and target database on which to run the query against. You can also define a target table that will contain the results of the transformation.
- Dimension loads are often done with the truncate-insert pattern where the target table is emptied before the load. Thus, you could also have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.

**3. Data Quality Operator**
- The final operator to create is the data quality operator, which is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result needs to be checked and if there is no match, the operator should raise an exception and the task should retry and fail eventually.
- For example one test could be a SQL statement that checks if certain column contains NULL values by counting all the rows that have NULL in the column. We do not want to have any NULLs so expected result would be 0 and the test would compare the SQL statement's outcome to the expected result.
