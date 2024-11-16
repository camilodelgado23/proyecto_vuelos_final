import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from conexion_BD import create_connection
 

def create_tables():
    """Crea las tablas de dimensiones y hechos en PostgreSQL."""
    connection = create_connection()
    
    if connection is not None:
        try:
            cursor = connection.cursor()
            
            # Crear tabla dim_date
            create_dim_date_query = """
            CREATE TABLE IF NOT EXISTS dim_date (
                date_key VARCHAR(100) PRIMARY KEY,
                month VARCHAR(50),
                day_of_week VARCHAR(50),
                dep_time_blk VARCHAR(100)
            );
            """
            
            # Crear tabla dim_airline
            create_dim_airline_query = """
            CREATE TABLE IF NOT EXISTS dim_airline (
                airline_key INT PRIMARY KEY,
                airline_name VARCHAR(250),
                icao_code VARCHAR(10),
                country_iso2 VARCHAR(10),
                status VARCHAR(50)
            );
            """
            
            # Crear tabla dim_airport
            create_dim_airport_query = """
            CREATE TABLE IF NOT EXISTS dim_airport (
                airport_key INT PRIMARY KEY,
                departing_airport VARCHAR(250),
                latitude FLOAT,
                longitude FLOAT
            );
            """
            
            # Crear tabla dim_weather
            create_dim_weather_query = """
            CREATE TABLE IF NOT EXISTS dim_weather (
                weather_key INT PRIMARY KEY,
                prcp FLOAT,
                snow FLOAT,
                tmax FLOAT,
                awnd FLOAT
            );
            """
            
            # Crear tabla de hechos flight_fact
            create_flight_fact_query = """
            CREATE TABLE IF NOT EXISTS flight_fact (
                flight_id SERIAL PRIMARY KEY,
                dep_del15 INT,
                distance_group INT,
                segment_number INT,
                concurrent_flights INT,
                number_of_seats INT,
                airport_flights_month INT,
                airline_flights_month INT,
                airline_airport_flights_month INT,
                avg_monthly_pass_airport FLOAT,
                avg_monthly_pass_airline FLOAT,
                plane_age FLOAT,
                dim_date_key VARCHAR(100) REFERENCES dim_date(date_key),
                dim_airline_key INT REFERENCES dim_airline(airline_key),
                dim_airport_key INT REFERENCES dim_airport(airport_key),
                dim_weather_key INT REFERENCES dim_weather(weather_key)
            );
            """
            
            # Ejecutar las consultas para crear cada tabla
            cursor.execute(create_dim_date_query)
            cursor.execute(create_dim_airline_query)
            cursor.execute(create_dim_airport_query)
            cursor.execute(create_dim_weather_query)
            cursor.execute(create_flight_fact_query)
            
            # Confirmar los cambios
            connection.commit()
            print("Tablas creadas exitosamente.")
        
        except (Exception, psycopg2.Error) as error:
            print(f"Error al crear las tablas: {error}")
        
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

if __name__ == "__main__":
    create_tables()