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
<img width="1639" alt="image" src="https://github.com/user-attachments/assets/7c73e61e-35c4-4028-bd6b-61839a8791cb" />

Extraction was thought out as following:
1. Having the data processing group in blocks, since paging in the API is needed (thats a caveat from the split being made that way, since huge amounts of extractions could be made like this, turning the DAG into a huge chart
2. Each block has specific pages being queried against the API
3. Results are saved to the bronze layer

There are 3 variables that control the extraction, created at startup, them being:\
<img width="930" alt="image" src="https://github.com/user-attachments/assets/96798479-a7e2-4c73-93c4-47a1d1487a00" />

| variable name        | description   |
| ------------- |:-------------:|
| BREWERY_API_PAGE_OFFSET          | determines if total records in API are different from current run, to materialize previously materialized pages      |
| BREWERY_API_PAGINATION_PARTITIONS      | determines which page partitions were already materialized     |
| BREWERY_API_TOTAL      | variable that gets the response from https://api.openbrewerydb.org/breweries/meta     |

The extraction blocks will use the BREWERY_API_TOTAL to generate the blocks and check variables BREWERY_API_PAGE_OFFSET / BREWERY_API_PAGINATION_PARTITIONS for already materialized pages (using BranchOperator when one page was already materialized). This was thought out to be able to request former data that was lost / deleted / changed with variables instead of modifying the code itself.

The first execution won't have any blocks created, since the pipeline doesn't know how many pages there are, hence creating the following structure:
<img width="1001" alt="image" src="https://github.com/user-attachments/assets/9b6eaaab-6fb8-4b9e-b1fa-4c0f7bf99ed0" />
<img width="1106" alt="image" src="https://github.com/user-attachments/assets/46c58945-f29c-4f31-a78f-ed06d06990d3" />

Upon execution, the task ***"brewery_api_extract_total"*** will query the API "/meta"/ to check for all records available, and create the blocks dynamically:
<img width="981" alt="image" src="https://github.com/user-attachments/assets/c2983293-0af8-4622-b79b-781e9bd18211" />

Rematerializing the DAG structure like so:
<img width="1063" alt="image" src="https://github.com/user-attachments/assets/00770af7-bc47-404b-8444-1fefb76fefda" />
<img width="389" alt="image" src="https://github.com/user-attachments/assets/29ca08bd-4e00-490a-b80f-693be55a10fe" />

The BranchOperator works checking if the task name associated with the page, for instance "branch_task_brewery_api_extract_offset_page_1" is present in the list variable "BREWERY_API_PAGINATION_PARTITION"
<img width="727" alt="image" src="https://github.com/user-attachments/assets/2089a5cd-2c89-41d6-ae6c-d452d96da80d" />\
if not, it will execute the task associated to querying the API with the defined page and save the JSON file to the bronze layer defined in the constants file under BRONZE_LAYER_PATH (that expands to path "data/bronze"
<img width="424" alt="image" src="https://github.com/user-attachments/assets/e6ebb4bf-9fe3-424e-8ee5-f2a69d2ac80d" />

After finishing up the DAG, the variables will look something like this:
<img width="896" alt="image" src="https://github.com/user-attachments/assets/437a0b7f-90a2-40eb-b319-a3480aeda76a" />
<img width="896" alt="image" src="https://github.com/user-attachments/assets/437a0b7f-90a2-40eb-b319-a3480aeda76a" />
<img width="901" alt="image" src="https://github.com/user-attachments/assets/6e57e626-03aa-4320-8181-31dc06ed718b" />

Let's say I'd like to only reprocess increments or a specific page, I wouldn't have to change any code, just the "BREWERY_API_PAGE_OFFSET" variable (for simulating incremental load of API data) or remove a partition from the "BREWERY_API_PAGINATION_PARTITIONS" variable (simulating a page reprocessing)\

For incremental example, let's say that the total API records was 8304 last run, and got 54 new records, totaling 8358, I'd have to materialize 2 new partitions.\
This scenario can be simulated by editing the "BREWERY_API_PAGE_OFFSET" variable and set its value to 8304\
<img width="738" alt="image" src="https://github.com/user-attachments/assets/79f8edc7-22f5-4a82-af31-90ceddb88634" />
<img width="763" alt="image" src="https://github.com/user-attachments/assets/965c49d3-0864-42db-9a72-966497bb149a" />

By triggering another DAG run, pages 166 and 167 will run this time (wheter they existed previously or not) and can be checked in the "extraction_block_5" portion of the DAG\
<img width="964" alt="image" src="https://github.com/user-attachments/assets/df17455b-1196-41a3-a234-ee4dbf77641b" />
all other extraction blocks are skipped, since they were already materialized in the previous run\
<img width="919" alt="image" src="https://github.com/user-attachments/assets/14e44042-eb72-4880-b493-a1632fb70a3a" />

This can be checked by seeing the underneath files creation dates, with command
```
ls -alt
```
<img width="815" alt="image" src="https://github.com/user-attachments/assets/73c51664-f3e7-4193-899b-38acef40ac33" />

Once the DAG is done with the pipeline, the variables are set again
<img width="768" alt="image" src="https://github.com/user-attachments/assets/05ac68d4-4151-41d1-b85a-52d8c3a1d096" />\
and the newly materialized partitions are added again
<img width="992" alt="image" src="https://github.com/user-attachments/assets/60a04b89-500f-416c-841f-99f3b994a5b6" />
<img width="1107" alt="image" src="https://github.com/user-attachments/assets/d25beee3-485d-4a1c-aa28-42bda7addce5" />


Transformation / Aggregation process:
The DAGs for both steps use the DuckDB to query data and save into delta tables.
1. A custom operator was created to query in memory and save to delta partitioned by location (a field made from the "country" column), with minor transformation being made in the silver layer.
2. Gold layer gets the newly created delta table and aggregates the result, saving it to the analytical 

Monitoring is made through the Airflow DAGs and tasks statuses and could be forwarded (using for example ***"on_failure_callback"*** on tasks to send them to Slack / email or similar)

- Silver / cleanup layer

<img width="1437" alt="image" src="https://github.com/user-attachments/assets/e4311c3b-66cf-4d3a-a7e4-435cc24ceadf" />

DAG is a query, loading files into delta format, by using custom DuckDB operator

<img width="711" alt="image" src="https://github.com/user-attachments/assets/9987c20a-edf3-4a49-b03a-81ceeeb3d750" />

and saves data to silver layer, partitioned by created "location" column

<img width="398" alt="image" src="https://github.com/user-attachments/assets/77ac8556-64e5-4497-bd22-87b81d758b63" />

- Gold / aggregation layer\

Uses the same DuckDB operator to query the delta table in memory, using SQL like syntax to aggregate the data\
<img width="1358" alt="image" src="https://github.com/user-attachments/assets/9cbbc140-99da-45e4-9731-4529f43b1c72" />

and save it to gold layer\
<img width="382" alt="image" src="https://github.com/user-attachments/assets/829fe3c4-4260-499b-96a7-ce6f4686f653" />






