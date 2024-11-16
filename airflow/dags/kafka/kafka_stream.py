from confluent_kafka import Producer
from json import dumps
import pandas as pd
import time

# Función que maneja la producción de mensajes desde el DataFrame hacia Kafka
def kafka_producer(dataframe):
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    message_counter = 0

    # Enviar los mensajes fila por fila
    for index in range(len(dataframe)):
        try:
            row = dataframe.iloc[index]
            message_json = dumps(row.to_dict())  # Convertir la fila en un JSON
            producer.produce("kafka_lab2", value=message_json)  # Enviar el mensaje
            producer.poll(0)  # Procesar cualquier evento pendiente de Kafka
            message_counter += 1

            # Cada 10 mensajes enviados, mostrar el contador
            if message_counter % 10 == 0:
                print(f"Mensajes enviados: {message_counter}")
            
            #time.sleep(0.01)  # Pausa breve entre mensajes

        except Exception as e:
            print(f"Error al enviar mensaje: {e}")
            break

    # Asegurar que todos los mensajes pendientes sean enviados
    producer.flush()
    print(f"Total de mensajes enviados: {message_counter}")