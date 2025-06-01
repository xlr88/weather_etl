from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
# from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.decorators import task
import requests
import json
from datetime import datetime, timedelta

LATITUDE = 17.4065
LONGITUDE = 78.4772
POSTGRES_CONN_ID = 'weather_connection'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1)
}

## DAG
with DAG(
    dag_id='mine_weather',
    default_args=default_args,
    description='Extract, Load, Transform Weather Data',
    schedule=timedelta(days=1),
    catchup=False
) as dag:

    @task()
    def extract_weather_data():
        http_hook = HttpHook(method='GET', http_conn_id=API_CONN_ID)
        # url = f'https://api.open-meteo.com/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&hourly=temperature_2m'
        url = f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        response = http_hook.run(endpoint=url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch data: {response.status_code}")
    
    @task()
    def transform_weather_data(weather_data):
        current_weather = weather_data.get('current_weather', {})
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data
    @task()
    def load_weather_data(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS mine_weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO mine_weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))
        conn.commit()
        cursor.close()
    
    # Define the task dependencies
    weather_data = extract_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)

    # Set the task dependencies
    # extract_weather_data() >> transform_weather_data(weather_data) >> load_weather_data(transformed_data)
     