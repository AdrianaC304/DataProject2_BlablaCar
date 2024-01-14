from confluent_kafka import Consumer, KafkaError
import streamlit as st
import json

# Configuración del consumidor
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'latest'
}

# Función para consumir y mostrar datos en Streamlit
def consumir_y_mostrar():
    consumer = Consumer(config)
    topic = 'rutas'
    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(1.0)  # Espera 1 segundo por nuevos mensajes

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        # Decodificar y mostrar el mensaje JSON en Streamlit
        mensaje_json = json.loads(msg.value().decode('utf-8'))
        st.write(mensaje_json)

# Aplicación Streamlit
def main():
    st.title("Consumidor de Kafka en Streamlit")
    st.write("Este es un ejemplo simple de un consumidor Kafka integrado en una aplicación Streamlit.")
    
    # Llamamos a la función para consumir y mostrar datos
    consumir_y_mostrar()

if __name__ == "__main__":
    main()
