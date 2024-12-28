from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import pandas as pd
import vertica_python
import pendulum
import logging

conn_info = {
    'host': 'vertica.tgcloudenv.ru',
    'port': 5433,
    'user': 'stv2024111143',
    'password': 'I3FhLQrF0s2dUzo',
    'database': 'dwh',
    'autocommit': True
}

def load_data_to_vertica():
    df = pd.read_csv('/data/group_log.csv')
    df['group_id'] = df['group_id'].astype(int)
    df['user_id'] = df['user_id'].astype(int)
    df['event'] = df['event'].astype(str)
    df['datetime'] = pd.to_datetime(df['datetime'], errors='coerce')
    df['user_id_from'] = df['user_id_from'].fillna(pd.NA) 
    df['user_id_from'] = df['user_id_from'].where(df['user_id_from'].notna(), None)

    with vertica_python.connect(**conn_info) as conn:
        cursor = conn.cursor()
        for index, row in df.iterrows():
            cursor.execute(
                "INSERT INTO STV2024111143__STAGING.group_log (group_id, user_id, user_id_from, event, datetime) VALUES (%s, %s, %s, %s, %s)",
                (row['group_id'], row['user_id'], row['user_id_from'], row['event'], row['datetime'])
            )

@dag(schedule_interval='@daily', start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), catchup=False)
def load_data_to_db():
    load_data = PythonOperator(
        task_id='load_data_to_vertica',
        python_callable=load_data_to_vertica
    )
dag_instance = load_data_to_db()