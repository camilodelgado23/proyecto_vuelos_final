import sys
import os
import json
import logging
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), './')))

from extraccion.extraccion_postgres import select_flights_data
from transformaccion.transform_flights_dataset import  drop_unnecesary_columns, standarizate_values , transform_numerical_to_categorical_data
from extraccion.extraccion_postgres import select_flights_data
from extraccion.extraccion_airlines_api import fetch_airlines_data , process_airlines_data , save_to_csv
from transformaccion.transform_api_airlines import filter_ByUnitedState , columns_to_remove , inputation_nulls
from transformaccion.merge_flights_and_airlines import standarizate_airline_name , merge_api_and_postgres_dataset ,delete_merged_unnecesary_columns
from transformaccion.dimensionals_tables_transform import create_flight_fact , create_dim_date , create_dim_airline , create_dim_weather , create_dim_airport ,create_dim_airport_foreign_key_on_fact_table
from carga.load_to_datawarehouse import load_to_postgresql
from kafka.kafka_stream import kafka_producer

def extract_data_from_postgres():
    flights=select_flights_data()
    print(flights.head(5))
    logging.info("csv read database successfully")
    return flights.to_json(orient='records')

def extract_data_from_airlines_api():
    airlines_api_data=fetch_airlines_data()
    logging.info("API read successfully")
    print("Initial data sample from API:")
    airlines_api_data = process_airlines_data(airlines_api_data)
    print(airlines_api_data.head(5))  
    print(airlines_api_data.columns)
    return airlines_api_data.to_json(orient='records')

def transform_airlines_api_data(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="extract_data_from_airlines_api")
    json_data = json.loads(str_data)
    print("Extracted JSON data:")
    print(json_data[:5]) 

    airlines_api_data = pd.json_normalize(data=json_data)
    logging.info("Data from API has started transformation process")
    print("Data before transformation:")
    print(airlines_api_data.head())
    print(airlines_api_data.columns)
    airlines_api_data=filter_ByUnitedState(airlines_api_data)
    airlines_api_data=columns_to_remove(airlines_api_data)
    airlines_api_data=inputation_nulls(airlines_api_data)
    logging.info("Finished the Api Transformations")
    print(airlines_api_data.head(5))
    print("Finished the Api Transformations")
    return airlines_api_data.to_json(orient='records')

def transform_flights_data_postgres(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="extract_data_from_postgres")

    if not str_data:
        logging.error("No data received from extract_data_from_postgres.")
        return
    json_data = json.loads(str_data)
    flights = pd.json_normalize(data=json_data)
    logging.info(f"data from csv has started transformation proccess")
    flights.columns = flights.columns.str.lower()
    flights=drop_unnecesary_columns(flights)
    flights=standarizate_values(flights)
    flights=transform_numerical_to_categorical_data(flights)
    print(flights)
    logging.info(f"data was transformed")
    return flights.to_json(orient='records')

def merge_airlines_api_and_flights_postgres(**kwargs):
    # extract data from flights transform
    ti = kwargs["ti"]
    logging.info( f"in the merge function")
    str_data = ti.xcom_pull(task_ids="transform_flights_data_postgres")
    json_data = json.loads(str_data)
    flights = pd.json_normalize(data=json_data)

    # extract data from airlines api transform
    str_data = ti.xcom_pull(task_ids="transform_airlines_api_data")
    json_data = json.loads(str_data)
    airlines_api_data = pd.json_normalize(data=json_data)
    
    logging.info("Data from MERGE has started transformation process")
    print("Data before transformation:")
    flights , airlines_api_data =standarizate_airline_name(flights , airlines_api_data )
    merged_df= merge_api_and_postgres_dataset(flights , airlines_api_data)
    merged_df= delete_merged_unnecesary_columns(merged_df)
    logging.info("Finished the Merge Transformations")
    return merged_df.to_json(orient='records')

def load_merged_data_in_datawarehouse(**kwargs):
    ti = kwargs["ti"]
    logging.info( f"in the merge function")
    str_data = ti.xcom_pull(task_ids="merge")
    json_data = json.loads(str_data)
    merge_flights_and_airlines = pd.json_normalize(data=json_data)
    
    logging.info( f"start dimensional transform")
    flight_fact=create_flight_fact(merge_flights_and_airlines)
    dim_date=create_dim_date(merge_flights_and_airlines)
    dim_airline=create_dim_airline(merge_flights_and_airlines)
    dim_weather=create_dim_weather(merge_flights_and_airlines)
    dim_airport=create_dim_airport(merge_flights_and_airlines)
    flight_fact=create_dim_airport_foreign_key_on_fact_table(dim_airport ,flight_fact)
    logging.info( f"finished dimensional transform")
    
    if not dim_date.empty :
        load_to_postgresql(dim_date,"dim_date")

    if not dim_airline.empty :
        load_to_postgresql(dim_airline,"dim_airline")

    if not dim_weather.empty :
        load_to_postgresql(dim_weather,"dim_weather")

    if not dim_airport.empty :
        load_to_postgresql(dim_airport,"dim_airport")

    if not flight_fact.empty :
        load_to_postgresql(flight_fact,"flight_fact")
    
    logging.info( f"finished dimensional load")
    return flight_fact.to_json(orient='records')


def kafka_stream_fact_table(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="load_merged_data_in_datawarehouse")

    json_data = json.loads(str_data)
    flight_fact = pd.json_normalize(data=json_data)
    logging.info(f"starting streaming data")
    res=kafka_producer(flight_fact)
    logging.info(f"finish stream data")
