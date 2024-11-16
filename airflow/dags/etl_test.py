import sys
import os
import json
import logging
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), './')))

from extraccion.extraccion_postgres import select_flights_data
from extraccion.extraccion_airlines_api import fetch_airlines_data , process_airlines_data , save_to_csv
from transformaccion.transform_flights_dataset import drop_unnecesary_columns, standarizate_values, transform_numerical_to_categorical_data
from transformaccion.transform_api_airlines import filter_ByUnitedState , columns_to_remove , inputation_nulls
from transformaccion.merge_flights_and_airlines import standarizate_airline_name , merge_api_and_postgres_dataset


def extract_data_from_postgres():
    flights=select_flights_data()
    logging.info("CSV read successfully")
    print("Initial data sample from CSV:")
    print(flights.head(5)) 
    print(flights.columns)
    return flights.to_json(orient='records')

def extract_data_from_airlines_api():
    airlines_api_data=fetch_airlines_data()
    logging.info("API read successfully")
    print("Initial data sample from API:")
    airlines_api_data = process_airlines_data(airlines_api_data)
    print(airlines_api_data.head(5))  
    print(airlines_api_data.columns)
    return airlines_api_data.to_json(orient='records')

def transform_airlines_api_data(airlines_api_data):
    json_data = json.loads(airlines_api_data)
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

def merge_airlines_api_and_flights_postgres(flights, airlines_api_data):
    json_data = json.loads(airlines_api_data)
    print("Extracted JSON data:")
    print(json_data[:5]) 

    airlines_api_data = pd.json_normalize(data=json_data)
    logging.info("Data from MERGE has started transformation process")
    print("Data before transformation:")
    print(airlines_api_data.head())
    print(airlines_api_data.columns)

    flights , airlines_api_data =standarizate_airline_name(flights , airlines_api_data )
    merged_df= merge_api_and_postgres_dataset(flights , airlines_api_data)

    logging.info("Finished the Merge Transformations")
    print(merged_df.head(5))
    return merged_df.to_json(orient='records')


def transform_data(flights):
    str_data=flights
    json_data = json.loads(str_data)
    print("Extracted JSON data:")
    print(json_data[:5])  # Muestra las primeras 5 entradas para validación

    flights = pd.json_normalize(data=json_data)
    logging.info("Data from CSV has started transformation process")
    print("Data before transformation:")
    print(flights.head())
    print(flights.columns)
    # Imprime el DataFrame inicial

    flights.columns = flights.columns.str.lower()
    # Applying transformations step-by-step
    flights = drop_unnecesary_columns(flights)
    print("Data after dropping unnecessary columns:")
    print(flights.head())
    print(flights.columns)

    flights = standarizate_values(flights)
    print("Data after standardizing values:")
    print(flights.head())
    print(flights.columns)
    flights = transform_numerical_to_categorical_data(flights)
    print("Data after transforming numerical to categorical data:")
    print(flights.head())

    logging.info("Data was transformed")
    return flights.to_json(orient='records')

def load_merged_dato_in_datawarehouse(merged_df):
    json_data = json.loads(merged_df)
    merged_df = pd.json_normalize(data=json_data)

    file_path = './csv/merged_flights_api_airlines.csv'

    if not os.path.exists(file_path):
        merged_df.to_csv(file_path, index=False)
        print(f"Archivo exportado a: {file_path}")
    else:
        print(f"El archivo ya existe en la ruta: {file_path}")


flights = extract_data_from_postgres()
flights= transform_data(flights)
airlines_api_data=extract_data_from_airlines_api()
airlines_api_data=transform_airlines_api_data(airlines_api_data)
# pasamos flights de json a dataframe
json_data = json.loads(flights)
flights = pd.json_normalize(data=json_data)
merged_data=merge_airlines_api_and_flights_postgres(flights,airlines_api_data)
load_merged_dato_in_datawarehouse(merged_data)