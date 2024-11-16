import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import pandas as pd
import psycopg2
from io import StringIO

def create_table(conn):
    with conn.cursor() as cur:
        query = """
        CREATE TABLE IF NOT EXISTS airplanes (
            registration_number VARCHAR(50),
            production_line VARCHAR(100),
            iata_type VARCHAR(50),
            model_name VARCHAR(50),
            model_code VARCHAR(50),
            icao_code_hex VARCHAR(10),
            iata_code_short VARCHAR(10),
            construction_number VARCHAR(50),
            test_registration_number VARCHAR(50),
            rollout_date TIMESTAMP,
            first_flight_date TIMESTAMP,
            delivery_date TIMESTAMP,
            registration_date TIMESTAMP,
            line_number VARCHAR(50),
            plane_series VARCHAR(50),
            airline_iata_code VARCHAR(10),
            airline_icao_code VARCHAR(10),
            plane_owner VARCHAR(100),
            engines_count INT,
            engines_type VARCHAR(50),
            plane_age NUMERIC,
            plane_status VARCHAR(50),
            plane_class VARCHAR(50)
        );
        """
        cur.execute(query)
        conn.commit()
        print("Tabla creada exitosamente.")

def clean_invalid_dates(df, date_columns, replacement_date):
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].replace("0000-00-00", replacement_date)
            df[col] = pd.to_datetime(df[col], errors='coerce')

            df[col].fillna(replacement_date, inplace=True)
        else:
            print(f"Columna '{col}' no encontrada en el DataFrame")

def load_cleaned_data(conn, df, table_name):
    try:
        buffer = StringIO()
        df.to_csv(buffer, header=False, index=False)  
        buffer.seek(0)  

        cursor = conn.cursor()

        cursor.copy_from(buffer, table_name, sep=',', null='')  

        conn.commit()

        print("Datos insertados correctamente.")

    except Exception as e:
        print(f"Error al insertar datos: {e}")
        conn.rollback()  
    finally:
        cursor.close()  


if __name__ == "__main__":
    conn = psycopg2.connect("dbname=proyecto1 user=postgres password=root host=localhost")

    cleaned_csv_file = "Proyecto_Vuelos_parte1/csv/cleaned_api_airplanes.csv"
    df = pd.read_csv(cleaned_csv_file)

    print(f"Columnas en el DataFrame: {df.columns.tolist()}")

    replacement_date = "1995-04-03"

    date_columns = ['rollout_date', 'first_flight_date', 'delivery_date', 'registration_date']  
    clean_invalid_dates(df, date_columns, replacement_date)

    table_name = 'airplanes'

    try:
        with conn.cursor() as cur:
            for i, row in df.iterrows():
                cur.execute(
                    f"INSERT INTO {table_name} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    tuple(row)
                )
            conn.commit()
        print("Datos insertados correctamente.")
    except Exception as e:
        print(f"Error al insertar datos: {e}")

    conn.close()