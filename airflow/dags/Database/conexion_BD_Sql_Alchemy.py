from sqlalchemy import create_engine
from credentials import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

def create_connection():
    """
    Crea y devuelve un motor de conexión SQLAlchemy para PostgreSQL.
    """
    DATABASE_URI = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    try:
        # Crear motor con compatibilidad futura para SQLAlchemy 2.0
        engine = create_engine(DATABASE_URI, future=True)
        print("Motor de conexión SQLAlchemy creado exitosamente.")
        return engine
    except Exception as e:
        print(f"Error al crear el motor de conexión SQLAlchemy: {e}")
        return None
