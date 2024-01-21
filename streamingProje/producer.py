import json
import xml.etree.ElementTree as ET
from google.cloud import pubsub_v1
import time

# Clase para la publicación en Pub/Sub
class PubSubProducer:
    def __init__(self, project_id, topic_name):
        self.project_id = project_id
        self.topic_name = topic_name
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_name)

    def publish_message(self, message):
        data = json.dumps(message).encode("utf-8")
        future = self.publisher.publish(self.topic_path, data)
        print(f"Publicado mensaje en Pub/Sub: {message}")
        return future

# Función para cargar coordenadas desde un archivo KML
def cargar_coordenadas_desde_kml(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    coordinates = []

    for coordinates_tag in root.findall('.//{http://www.opengis.net/kml/2.2}coordinates'):
        coordinates_text = coordinates_tag.text.strip()

        for coord_set in coordinates_text.split('\n'):
            coordinates.append(coord_set)

    return coordinates

# Función para convertir coordenadas a formato JSON
def convertir_a_json(coordinates):
    coordinates_json = []
    for index, coord_text in enumerate(coordinates, start=1):
        lat, lon, alt = [float(coord) for coord in coord_text.split(',')]
        coordinates_json.append({'index': index, 'latitud': lat, 'longitud': lon})

    return coordinates_json

def main():
    # Ruta al archivo KML
    file_path = 'ruta_1.kml'

    # Cargar coordenadas desde el archivo KML
    coordinates = cargar_coordenadas_desde_kml(file_path)

    # Convertir a formato JSON
    coordinates_json = convertir_a_json(coordinates)

    # Crear una instancia de la clase PubSubProducer
    pubsub_producer = PubSubProducer(project_id='dataflow-1-411618', topic_name='coches')

    # Enviar coordenadas a través de Pub/Sub
    for coord_message in coordinates_json:
        pubsub_producer.publish_message(coord_message)
        time.sleep(1)  # Esperar 1 segundo entre mensajes

    print("Todos los mensajes han sido enviados a Pub/Sub.")

if __name__ == "__main__":
    main()