
from datetime import timedelta,datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sqlite3
import os
from  Spotify_ETL import get_top_songs, conectar_Redshift, insert_data
# Obtener el dag del directorio
dag_path = os.getcwd()

default_args = {
    'start_date': datetime(2023, 5, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

ingestion_dag = DAG(
    dag_id='ingestion_data',
    default_args=default_args,
    description='Agrega datos de las 50 canciones mas escuchadas en spotify',
     schedule_interval=timedelta(days=1),
    catchup=False
)

task_1 = PythonOperator(
    task_id='transformar_data',
    python_callable=get_top_songs,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=ingestion_dag,
)

task_2 = PythonOperator(
    task_id='load_conection',
    python_callable=conectar_Redshift,
    dag=ingestion_dag,
)

task_3 = PythonOperator(
    task_id='insert_data',
    python_callable=insert_data,
    dag=ingestion_dag,
)


task_1 >> task_2 >> task_3