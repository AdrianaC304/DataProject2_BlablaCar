from google.cloud import pubsub_v1
import json
import random
import time

def generate_random_name():
    names = ["Alice", "Bob", "Charlie", "David", "Eva", "Frank", "Grace", "Harry"]
    return {"nombre": random.choice(names)}

def publish_messages():
    project_id = "dataflow-1-411618"
    topic_name = "new_topic"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    for _ in range(10):  # Enviar 10 nombres aleatorios
        message = generate_random_name()
        data = json.dumps(message).encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(f"Publicado mensaje: {message}")
        time.sleep(1)  # Esperar 1 segundo entre mensajes

    print("Todos los mensajes han sido enviados.")

if __name__ == "__main__":
    publish_messages()
