#!/bin/bash
set -e  # Exit immediately if a command fails
set -x  # Print each command before executing it (debug mode)

echo "Initializing Airflow database..."
airflow db migrate

# Starts some variables
airflow variables set BREWERY_API_PAGE_OFFSET 0
airflow variables set BREWERY_API_PAGINATION_PARTITIONS []
airflow variables set BREWERY_API_TOTAL '{"total":0}'

echo "Creating admin user if not exists..."
airflow users list | grep -q "admin" || airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

echo "Starting Airflow webserver..."
exec airflow webserver
