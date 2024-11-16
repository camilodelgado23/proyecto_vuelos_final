from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
#from etl_pipeline import read_csv, read_db, transform_spotify, grammys_transform_db, merge, load
from etl_pipeline import  extract_data_from_postgres , transform_flights_data_postgres , extract_data_from_airlines_api ,transform_airlines_api_data , merge_airlines_api_and_flights_postgres ,load_merged_data_in_datawarehouse , kafka_stream_fact_table


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 7),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'etl_dag',
    default_args=default_args,
    description='final-project-etl',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
) as dag:

    extract_data_from_postgres = PythonOperator(
        task_id='extract_data_from_postgres',
        python_callable=extract_data_from_postgres,
    )
    
    extract_data_from_airlines_api = PythonOperator(
        task_id='extract_data_from_airlines_api',
        python_callable=extract_data_from_airlines_api,
    )

    transform_flights_data_postgres = PythonOperator(
        task_id='transform_flights_data_postgres',
        python_callable=transform_flights_data_postgres,
        )
    
    transform_airlines_api_data = PythonOperator(
        task_id='transform_airlines_api_data',
        python_callable=transform_airlines_api_data,
        )
    
    merge_airlines_api_and_flights_postgres = PythonOperator(
        task_id='merge',
        python_callable=merge_airlines_api_and_flights_postgres,
    )

    load_merged_data_in_datawarehouse = PythonOperator(
        task_id='load_merged_data_in_datawarehouse',
        python_callable=load_merged_data_in_datawarehouse,
    )
    
    kafka_stream_fact_table = PythonOperator(
        task_id='kafka_stream_fact_table',
        python_callable=kafka_stream_fact_table,
       )
   

    extract_data_from_postgres >> transform_flights_data_postgres >> merge_airlines_api_and_flights_postgres
    extract_data_from_airlines_api >> transform_airlines_api_data  >> merge_airlines_api_and_flights_postgres >> load_merged_data_in_datawarehouse >> kafka_stream_fact_table