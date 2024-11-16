from confluent_kafka.admin import AdminClient, NewTopic

def create_kafka_topic():
    admin_client = AdminClient({
        "bootstrap.servers": "localhost:9092"
    })
    
    topic = "kafka_lab2"
    new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
    
    fs = admin_client.create_topics([new_topic])
    
    for topic, f in fs.items():
        try:
            f.result()  # Verifica si se creó con éxito
            print(f"Tópico {topic} creado exitosamente.")
        except Exception as e:
            print(f"Fallo al crear el tópico {topic}: {e}")

# Llama a la función para crear el tópico
create_kafka_topic()
