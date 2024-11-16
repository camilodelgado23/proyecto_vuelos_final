from confluent_kafka import Consumer
from json import loads
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from dash import Dash, html, dcc
from dash.dependencies import Input, Output
import threading
from collections import deque
import time

# Configuración de la cola de datos para mantener los últimos 100 mensajes
data_queue = deque(maxlen=100)

# Inicialización de la aplicación Dash
app = Dash(__name__)

# Definición del layout de la aplicación
app.layout = html.Div([
    html.H1("Monitoreo de Vuelos en Tiempo Real", 
            style={'textAlign': 'center', 'color': '#2c3e50', 'marginBottom': 20}),
    
    # Contador de vuelos monitoreados
    html.Div([
        html.H3(id='data-counter', 
                style={'textAlign': 'center', 'color': '#e74c3c'})
    ]),
    
    # Gráficas
    html.Div([
        # Fila 1
        html.Div([
            dcc.Graph(id='delay-pie-chart', 
                     style={'width': '48%', 'display': 'inline-block', 'margin': '1%'},
                     config={'displayModeBar': False}),
            dcc.Graph(id='distance-delays', 
                     style={'width': '48%', 'display': 'inline-block', 'margin': '1%'},
                     config={'displayModeBar': False}),
        ]),
        
        # Fila 2
        html.Div([
            dcc.Graph(id='plane-age-scatter', 
                     style={'width': '48%', 'display': 'inline-block', 'margin': '1%'},
                     config={'displayModeBar': False}),
            dcc.Graph(id='monthly-delays', 
                     style={'width': '48%', 'display': 'inline-block', 'margin': '1%'},
                     config={'displayModeBar': False}),
        ]),

        # Gráfico de Retraso Promedio por Día de la Semana
        html.Div([ 
            dcc.Graph(id='avg-delay-by-day', 
                     style={'width': '100%', 'display': 'inline-block', 'margin': '1%'},
                     config={'displayModeBar': False}),
        ]),

        # Gráfico de Pasajeros vs Retrasos
        html.Div([ 
            dcc.Graph(id='passengers-delay', 
                     style={'width': '100%', 'display': 'inline-block', 'margin': '1%'},
                     config={'displayModeBar': False}),
        ]),

        # Intervalo de actualización
        dcc.Interval(
            id='interval-component',
            interval=1000,  # Intervalo de actualización en milisegundos (1 segundo)
            n_intervals=0
        ),
    ])
])

# Función para limpiar las fechas y convertirlas correctamente
def convert_date(row):
    try:
        # Si el formato es algo como 'January-Friday', lo convertimos a una fecha
        # Asumimos un día genérico (por ejemplo, el 1 de cada mes)
        month_to_num = {
            "January": "01", "February": "02", "March": "03", "April": "04",
            "May": "05", "June": "06", "July": "07", "August": "08",
            "September": "09", "October": "10", "November": "11", "December": "12"
        }
        
        # Separar el mes y el día de la semana
        parts = row.split('-')
        if len(parts) == 2:
            month_str, day_of_week = parts
            # Usar el primer día del mes para la conversión
            month_num = month_to_num.get(month_str, "01")
            return pd.to_datetime(f"2024-{month_num}-01", errors='raise')
        return pd.NaT
    except Exception as e:
        print(f"Error al convertir {row}: {e}")
        return pd.NaT  # Not a Time, valor nulo en pandas para fechas

# Callback para actualizar las gráficas
@app.callback(
    [Output('data-counter', 'children'),
     Output('delay-pie-chart', 'figure'),
     Output('distance-delays', 'figure'),
     Output('plane-age-scatter', 'figure'),
     Output('monthly-delays', 'figure'),
     Output('avg-delay-by-day', 'figure'),
     Output('passengers-delay', 'figure')],
    [Input('interval-component', 'n_intervals')]
)
def update_graphs(n):
    # Convertir los datos de la cola a un DataFrame
    df = pd.DataFrame(list(data_queue))
    
    if df.empty:
        return "Esperando datos...", *[go.Figure() for _ in range(7)]
    
    counter_text = f"Vuelos monitoreados: {len(df)}"
    
    # Gráfica de retrasos (Pastel)
    pie_fig = px.pie(
        df, 
        names=df['dep_del15'].map({1: 'Retrasado', 0: 'A tiempo'}),
        title='Estado de Vuelos',
        color_discrete_sequence=['#2ecc71', '#e74c3c']
    )
    pie_fig.update_traces(textinfo='percent+label')
    pie_fig.update_layout(transition_duration=100)

    # Gráfica de retrasos por distancia (Barras)
    distance_fig = px.bar(
        df.groupby('distance_group')['dep_del15'].mean().reset_index(),
        x='distance_group',
        y='dep_del15',
        title='Retrasos por Distancia'
    )
    distance_fig.update_layout(transition_duration=100)

    # Gráfico de dispersión (Edad del avión vs Retrasos)
    age_fig = px.scatter(
        df,
        x='plane_age',
        y='dep_del15',
        title='Edad del Avión vs Retrasos',
        color='dep_del15'
    )
    age_fig.update_layout(transition_duration=100)

    # Gráfica de retrasos mensuales
    df['month'] = df['dim_date_key'].str.split('-').str[0]
    monthly_fig = px.line(
        df.groupby('month')['dep_del15'].mean().reset_index(),
        x='month',
        y='dep_del15',
        title='Tendencia Mensual'
    )
    monthly_fig.update_layout(transition_duration=100)

    # Limpiar las fechas en la columna dim_date_key
    df['dim_date_key'] = df['dim_date_key'].apply(convert_date)
    
    # Si se logró convertir, extraer el día de la semana
    df['day_of_week'] = df['dim_date_key'].dt.day_name()  # Extraer el nombre del día de la semana
    
    # Calcular el retraso promedio por día de la semana
    day_of_week_fig = px.bar(
        df.groupby('day_of_week')['dep_del15'].mean().reset_index(),
        x='day_of_week',
        y='dep_del15',
        title='Retraso Promedio por Día de la Semana',
        category_orders={"day_of_week": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]}
    )
    day_of_week_fig.update_layout(transition_duration=100)

    # Gráfico de Pasajeros vs Retrasos
    passengers_fig = px.scatter(
        df, 
        x='avg_monthly_pass_airport', 
        y='dep_del15', 
        title='Pasajeros Mensuales Promedio vs Retrasos',
        labels={'avg_monthly_pass_airport': 'Pasajeros Promedio por Aeropuerto', 'dep_del15': 'Retraso Promedio'},
        trendline="ols"  # Ajustar línea de tendencia
    )
    passengers_fig.update_layout(transition_duration=100)

    return counter_text, pie_fig, distance_fig, age_fig, monthly_fig, day_of_week_fig, passengers_fig


# Función del consumidor de Kafka
def kafka_consumer():
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': 'my-group-1',
        'auto.offset.reset': 'earliest'
    })

    consumer.subscribe(['kafka_lab2'])
    print("Iniciando monitoreo de vuelos...")

    try:
        while True:
            msg = consumer.poll(1.0)  # Esperar hasta 1 segundo por mensajes
            if msg is None:
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            try:
                message_value = loads(msg.value().decode('utf-8'))
                if 'dep_del15' in message_value and 'distance_group' in message_value:
                    data_queue.append(message_value)  # Agregar el mensaje a la cola
                else:
                    print(f"Mensaje no válido: {message_value}")
            except Exception as e:
                print(f"Error al procesar mensaje: {e}")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

# Iniciar el consumidor en un hilo
if __name__ == "__main__":
    consumer_thread = threading.Thread(target=kafka_consumer, daemon=True)
    consumer_thread.start()
    
    # Iniciar Dash sin modo debug para mejor rendimiento
    app.run_server(debug=False, port=8050, host='0.0.0.0')
