import psycopg2
import sys
import os

#Agregamos la ruta a la raíz del proyecto al sys.path para facilitar importaciones 
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from credentials import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

def create_connection():
    """Crea una conexión a la base de datos PostgreSQL."""
    try:
        connection = psycopg2.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        print("Conexión exitosa a la base de datos PostgreSQL.")
        return connection
    except psycopg2.OperationalError as e:
        print(f"Error al conectar con PostgreSQL: {e}")
        return None

def test_connection():
    """Prueba la conexión y realiza una consulta de prueba."""
    connection = create_connection()
    
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT version();")
            record = cursor.fetchone()
            print(f"Conectado a - {record}\n")
        except psycopg2.Error as e:
            print(f"Error al ejecutar la consulta: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

if __name__ == "__main__":
    test_connection()