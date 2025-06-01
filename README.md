# weather_etl
weather etl orchestrated using airflow

run the following commands to start the airflow with postgres and pgAdmin.

mkdir -p ./dags ./logs ./plugins ./config
echo -e "AIRFLOW_UID=$(id -u)" > .env

AIRFLOW_UID=50000

docker compose up airflow-init

docker compose up

These commands will bring up the airflow and run the DAGs from dag section of airflow.
