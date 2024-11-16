# Proyecto: DELAYED FLIGHTS :airplane:

Por:  
- Camilo Jose Delgado Bolaños 
-  Jose Daniel Carrera 

## Contexto :page_facing_up:

Este Dataset fue extraido de la pagina web kaggle una plataforma de competencia de ciencia de datos y una comunidad en línea para científicos de datos y profesionales del aprendizaje automático de Google LLC.

Este Dataset contiene informacion sobre vuelos retrasados en el año 2019 con informacion sobre el clima, aereopuerto entre otros, para un mayor detalle puede visitar la pagina de donde se extrajo el dataset en el siguiente enlace: https://www.kaggle.com/datasets/threnjen2019-airline-delays-and-cancellations?select=train.csv

Este Dataset fue complementado con la informacion extraida de la siguiente API: https://api.aviationstack.com/v1/airplanes la cual ofrece acceso sencillo a datos globales de aviación, incluyendo vuelos en tiempo real, históricos y futuros, así como rutas aéreas y otra información actualizada.

## Descripcion de columnas Dataset :clipboard:

1. **Información Temporal:**

- **month:** Mes del año.
- **day_of_week:** Día de la semana.
- **dep_block_hist:** Bloque de salida.
2. **Variables de Destino (Target):**
- **dep_del15:** Indicador binario de un retraso de salida de más de 15 minutos (1 es sí).
3. **Características del Vuelo:**
- **distance group:** Grupo de distancia que volará la aeronave.
- **segment_number:** Número de segmento en el que se encuentra la aeronave para el día.
- **concurrent_flights:** Número de vuelos simultáneos que salen del aeropuerto en el mismo bloque de salida.
- **number_of_Sseats:** Número de asientos en la aeronave.
- **plane_age:** Antigüedad de la aeronave que sale.
4. **Características del Transportista y Aeropuerto:**
- **carrier_name:** Nombre del transportista.
- **airport_fligths_month:** Promedio de vuelos del aeropuerto por mes.
- **airline_fligths_month:** Promedio de vuelos de la aerolínea por mes.
- **airline_airport_fligths_month:** Promedio de vuelos por mes para la aerolínea y el aeropuerto.
- **avg_monthly_pass_airport:** Promedio de pasajeros para el aeropuerto de salida para el mes.
- **avg_monthly_pass_airline:** Promedio de pasajeros por aerolínea para el mes.
- **flt_attendants_per_pass**: Auxiliares de vuelo por pasajero de la aerolínea.
- **ground_serv_per_pass**: Empleados de servicio en tierra (mostrador de servicio) por pasajero de la aerolínea.
5. **Información Geográfica:**
- **departing_airport:** Aeropuerto de salida.
- **latitude:** Latitud del aeropuerto de salida.
- **longitude:** Longitud del aeropuerto de salida.
- **previous_airport:** Aeropuerto anterior del que salió la aeronave.
6. **Condiciones Meteorológicas:**
- **prcp:** Pulgadas de precipitación del día.
- **snow:** Pulgadas de nevadas del día.
- **snwd:** Pulgadas de nieve en tierra del día.
- **tmax:** Temperatura máxima del día.
- **awnd:** Velocidad máxima del viento del día

## Descripcion de columnas API :computer:

- **results:** Devuelve un array de resultados, donde cada entrada representa los datos de una aerolínea en particular. Cada elemento del array incluye los detalles de las aerolíneas como se describe a continuación.

- **airline_name:** Devuelve el nombre completo de la aerolínea.

- **iata_code:** Devuelve el código IATA de la aerolínea, que es un código de dos letras único asignado por la Asociación Internacional de Transporte Aéreo (IATA).

- **iata_prefix_accounting:** Devuelve el prefijo o código contable de la aerolínea, utilizado en procesos administrativos y financieros de la industria.

- **icao_code:** Devuelve el código ICAO de la aerolínea, un código de tres letras asignado por la Organización de Aviación Civil Internacional (ICAO), que suele utilizarse en procedimientos de vuelo y control de tráfico aéreo.

- **callsign:** Devuelve el indicativo de llamada (callsign) de la aerolínea, usado en comunicaciones de radio durante las operaciones de vuelo.

- **type:** Devuelve el tipo de aerolínea, indicando si es, por ejemplo, una aerolínea comercial, de carga, de bajo costo, regional, etc.

- **status:** Devuelve el estado actual de la aerolínea, indicando si está operativa, inactiva o ha sido suspendida.

- **fleet_size:** Devuelve el tamaño de la flota de la aerolínea, es decir, el número total de aeronaves que posee o utiliza para sus operaciones.

- **fleet_average_age:** Devuelve la edad promedio de las aeronaves en la flota, lo cual es un indicador del envejecimiento de la flota.

- **date_founded:** Devuelve el año en que se fundó la aerolínea.

- **hub_code:** Devuelve el código del hub o centro de conexiones principal de la aerolínea, donde tiene su base principal de operaciones.

- **country_name:** Devuelve el nombre del país de origen de la aerolínea.

- **country_iso2:** Devuelve el código ISO de dos letras del país de origen de la aerolínea, utilizado para identificar el país de manera estandarizada (por ejemplo, "US" para Estados Unidos).

## Herramientas Usadas :computer:

- **Python**: 
  - **Python Scripting**: Para automatizar tareas como la inserción de datos en bases de datos, y la exportación de archivos. Visual Studio Code (VS Code): Como entorno para escribir y ejecutar código Python.

- **Jupyter Notebook:** Para desarrollo interactivo de código, exploración de datos, y ejecución de scripts.
- **Virtual Environment (venv):** Para gestionar dependencias y aislar el entorno de desarrollo.
- **Pandas:** Para manipulación y análisis de datos.
- **SQLAlchemy:** Para poder interactuar con bases de datos relacionales utilizando objetos de Python en lugar de escribir consultas SQL directamente.
- **Git LFS:** Para manejar archivos grandes (En este caso datasets) y para que se puedan subir al repositorio GitHub sin problema.

**PostgreSQL:**

- **pgAdmin:** Para gestión y administración de bases de datos PostgreSQL

- **Git:** Para control de versiones y seguimiento de cambios en el proyecto.

- **GitHub:** Para alojar el repositorio del proyecto, gestionar el control de versiones, y colaborar en el desarrollo del proyecto.

- **Power BI:** Para la visualizacion de Datos

- **Docker:** Para orquestar y configurar dos servicios relacionados con Apache Kafka
- **ZooKeeper:** Para configurar el servicio de coordinación que Apache Kafka que utiliza para la gestión de clústeres y la sincronización de datos.
- **Kafka:** Como broker para procesar y manejar mensajes.
- **plotly Dash:** Para realizar el dashboar en tiempo Real.

## Instrucciones para la ejecucion: :pencil:

### Requerimientos :point_left:
- Python: https://www.python.org/downloads/
- PostgreSQL: https://www.postgresql.org/
- PowerBI: https://www.microsoft.com/es-es/download/details.aspx?id=58494
- pgAdmin(Opcional):https://www.pgadmin.org/
- Docker: https://www.docker.com/
- Kafka: https://kafka.apache.org/
- plotly Dash: https://dash.plotly.com/

Clonamos el repositorio en nuestro entorno

```bash
  git clone https://github.com/camilodelgado23/proyecto_vuelos_final.git
```

Vamos al repositorio clonado

```bash
  cd proyecto_vuelos_final
```

Instalamos el entrono virtual donde vamos a trabajar

```bash
  Python -m venv venv 
```

Iniciamos el entorno

```bash
  source venv/bin/activate
```

Instalamos las librerias necesarias almacenadas en el archivo requirements.txt

```bash
  pip install -r requirements.txt
```

Creamos la Base de Datos en PostgreSQL

![Texto alternativo](https://imagenes.notion.site/image/https%3A%2F%2Fprod-files-secure.s3.us-west-2.amazonaws.com%2Fb687bcac-6636-49ac-8ce3-1adf66aa571c%2Ff89f0ee0-6df7-499d-965d-87a335bc5d80%2Fimage.png?table=block&id=5f9300c4-66f8-47f7-940d-1e04ad64223d&spaceId=b687bcac-6636-49ac-8ce3-1adf66aa571c&width=980&userId=&cache=v2)

Creamos el archivo credentials.py donde almacenaremos las credenciales para conectarnos a la Base de Datos, puede seguir la siguiente estructura

```bash
    DB_USER = 'tu_usuario'
    DB_PASSWORD = 'tu_contraseña'
    DB_HOST = 'tu_host'
    DB_PORT = 'tu_puerto'
    DB_NAME = 'tu_base_datos'
```
Podemos probar si las credenciales son correctas ejecutando nuestro archivo conexion.py.

## Airflow
Escalamos el Airflow 

```bash
  airflow standalone
```
una vez adentro iniciamos nos dirigimos a p://localhost:8080 e iniciamos secion con el usuario y contraseña que nos aparece en las líneas de código, y en la seccion de Dags buscamos el dag llamado etl_dag

![Texto alternativo](https://imagenes.notion.site/image/https%3A%2F%2Fprod-files-secure.s3.us-west-2.amazonaws.com%2Fb687bcac-6636-49ac-8ce3-1adf66aa571c%2Fb69d6a98-9834-4889-b189-d5e8069dd0be%2FWhatsApp_Image_2024-11-15_at_2.07.04_PM.jpeg?table=block&id=14038733-ed67-8008-a65f-c2010c131cf6&spaceId=b687bcac-6636-49ac-8ce3-1adf66aa571c&width=1420&userId=&cache=v2)

![Texto alternativo](https://imagenes.notion.site/image/https%3A%2F%2Fprod-files-secure.s3.us-west-2.amazonaws.com%2Fb687bcac-6636-49ac-8ce3-1adf66aa571c%2F9da1b03e-f384-47a6-bc0b-1a4f5026ac5f%2FWhatsApp_Image_2024-11-15_at_10.32.56_PM.jpeg?table=block&id=14038733-ed67-80df-8738-e3450d9b77b1&spaceId=b687bcac-6636-49ac-8ce3-1adf66aa571c&width=1420&userId=&cache=v2)

luego corremos el docker para el funcionamiento del kafka 
```bash
docker compose up -d
```

Finalmente Podemos ver el funcionamiento del dashboard en tiempo Real como se puede observar en el siguiente video, dele clik a la imagen para redireccionarlo al video en drive. 

## Video
[![Ver Video](https://imagenes.notion.site/image/https%3A%2F%2Fprod-files-secure.s3.us-west-2.amazonaws.com%2Fb687bcac-6636-49ac-8ce3-1adf66aa571c%2F98477692-e812-4bec-b74d-40244e053626%2Fimage.png?table=block&id=14038733-ed67-8022-9bf6-d7ed1ab6b051&spaceId=b687bcac-6636-49ac-8ce3-1adf66aa571c&width=1310&userId=&cache=v2)](https://drive.google.com/file/d/1sRuprHRY_VacPTuxca8XeINJRowTnfR-/view?usp=sharing)

## Gracias por revisar este proyecto :wave:# flights
# flights
