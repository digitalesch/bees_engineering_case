# BEES Data Engineering Case
The repository contains data pipelines for extracting and transforming brewery data from public API [Brewery API](https://www.openbrewerydb.org)

Solution was thought out using the following tools:
- Airflow (for orchestration)
- DuckDB (for Delta table creation and exploration)
- Docker (for containerization)

## Requirements to run the application

The solution uses Docker and ***docker-compose*** (or ***"docker compose"***) utility to better manage and distribute the code for ease of use.

### Prerequesites
1. Docker installation
2. ***"docker-compose"*** or ***"docker compose"*** utility

### Steps to run the solution
1. Download code (git clone repository)
2. Change directory to downloaded folder
3. Use ***"docker-compose"*** or ***"docker compose"*** utility with command
```
docker-compose up
```
or
```
docker-compose up -d
```

Parameter "-d" starts up as background processes and terminal is not attached to container, so output is not shown during initialization

## Code folder structure

| folder        | description   |
| ------------- |:-------------:|
| dags          | contains DAG definitions for extracting and transforming data     |
| data      | contains extracted / transformed data     |
| scripts      | contains script for container initialization and python packages required installs     |

DAGs were split into three layers:
- bronze (extracts data from API in raw format, getting JSON files as output)
- silver (transforms raw data into columnar format, using Delta kernel from DuckDB)
- gold (aggregates silver layer results)

When initialized, home / DAG screen might look similar to this

Extraction was thought out as following:
1. Having the data processing group in blocks, since paging in the API is needed (thats a caveat from the split being made that way, since huge amounts of extractions could be made like this, turning the DAG into a huge chart
2. Each block has specific pages being queried against the API
3. Results are saved to the bronze layer

Transformation / Aggregation process:
The DAGs for both steps use the DuckDB to query data and save into delta tables.
1. A custom operator was created to query in memory and save to delta partitioned by location (a field made from the "country" column), with minor transformation being made in the silver layer.
2. Gold layer gets the newly created delta table and aggregates the result, saving it to the analytical 

Monitoring is made through the Airflow DAGs and tasks statuses and could be forwarded (using for example ***"on_failure_callback"*** on tasks to send them to Slack / email or similar)

