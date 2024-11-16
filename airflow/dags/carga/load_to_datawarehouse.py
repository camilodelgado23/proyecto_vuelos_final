import pandas as pd
from sqlalchemy import text
from Database.conexion_BD_Sql_Alchemy import create_connection

def load_to_postgresql(df, table_name):
    """
    Carga un DataFrame en una tabla de PostgreSQL utilizando SQLAlchemy.
    """
    engine = create_connection()  # Crear motor SQLAlchemy

    if engine:
        try:
            # Conexión explícita para operaciones SQL directas
            with engine.connect() as conn:
                # Truncar tabla antes de cargar datos
                conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE;"))
                print(f"Datos en la tabla {table_name} truncados exitosamente.")
            
            # Cargar datos del DataFrame a la tabla usando el motor
            df.to_sql(
                table_name,
                con=engine,  # Usar motor directamente
                if_exists='append',
                index=False,
                method='multi'  # Para inserciones optimizadas
            )
            print(f"Dddddatos subidos a la tabla {table_name} exitosamente.")
        except Exception as e:
            print(f"Error al subir los datos a la tabla {table_name}: {e}")
        finally:
            engine.dispose()  # Liberar recursos del motor
    else:
        print("No se pudo establecer conexión con la base de datos.")
