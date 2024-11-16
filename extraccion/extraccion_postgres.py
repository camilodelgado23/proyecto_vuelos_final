import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), './')))

import pandas as pd
from Database.conexion_BD import create_connection


def select_flights_data():
    conn = create_connection()
    
    try:
        query = "SELECT * FROM flights"  
        df_flights = pd.read_sql(query, conn)

        print(df_flights.head())
        return df_flights
    except Exception as e:
        print(f"Error al seleccionar datos: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    flights_data = select_flights_data()
