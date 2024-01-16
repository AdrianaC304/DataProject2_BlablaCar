import json
import os
import xml.etree.ElementTree as ET
from confluent_kafka import Producer
import time
import subprocess

# Ejecutar el comando para crear el topic
comando_creacion_topic = [
    'docker-compose', 'exec', 'kafka',
    'kafka-topics', '--create',
    '--topic', 'rutas',
    '--partitions', '1',
    '--replication-factor', '1',
    '--if-not-exists',
    '--bootstrap-server', 'localhost:9092'
]

subprocess.run(comando_creacion_topic)


# Retrasa la ejecucion del script 10 segundos
time.sleep(5)




#######################################################################
#######################################################################

class CoordinateProducer:

    def __init__(self, bootstrap_servers='localhost:9092'):
        self.config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'python-coordinate-producer'
        }
        self.producer = Producer(self.config)
        self.topic_kafka = 'rutas'

    def send_coordinates(self, coordinates):
        for coord in coordinates:
            # Convertir el diccionario a formato JSON
            json_coord = json.dumps(coord)

            # Enviar el mensaje a Kafka
            self.producer.produce(self.topic_kafka, value=json_coord)
            self.producer.flush()

            # Imprimir las coordenadas y el índice por consola
            print(f"Index: {coord['index']}, {coord['latitud']}, {coord['longitud']}")

            # Esperar 1 segundo antes de enviar el siguiente
            time.sleep(1)





def cargar_coordenadas_desde_kml(file_path):
    # Cargar el archivo KML
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Inicializar la lista de coordenadas
    coordinates = []

    # Encuentra todas las coordenadas dentro de las etiquetas <coordinates>
    for coordinates_tag in root.findall('.//{http://www.opengis.net/kml/2.2}coordinates'):
        coordinates_text = coordinates_tag.text.strip()

        # Dividir las coordenadas y manejar cada conjunto por separado
        for coord_set in coordinates_text.split('\n'):
            coordinates.append(coord_set)

    return coordinates




def convertir_a_json(coordinates):
    # Convertir la lista de coordenadas a una lista de diccionarios
    coordinates_json = []
    for index, coord_text in enumerate(coordinates, start=1):
        lat, lon, alt = [float(coord) for coord in coord_text.split(',')]
        coordinates_json.append({'index': index, 'latitud': lon, 'longitud': lat})

    return coordinates_json




def guardar_json_en_archivo(coordinates_json, output_file='coordinates.json'):
    with open(output_file, 'w') as json_file:
        json.dump(coordinates_json, json_file, indent=2)








def main():
    # Ruta a la carpeta "rutas"
    carpeta_rutas = '/Users/adrianacamposnarvaez/Documents/GitHub/DataProject2_BlablaCar/Rutas'

    # Procesar archivos en la carpeta
    for root, dirs, files in os.walk(carpeta_rutas):
        for file_name in files:
            if file_name.endswith('.kml'):
                file_path = os.path.join(root, file_name)

                # Cargar coordenadas desde el archivo KML
                coordinates = cargar_coordenadas_desde_kml(file_path)

                # Convertir a formato JSON
                coordinates_json = convertir_a_json(coordinates)

                # Guardar JSON en archivo
                output_json_file = file_path[:-4] + '_coordinates.json'
                guardar_json_en_archivo(coordinates_json, output_json_file)

                # Crear una instancia de la clase CoordinateProducer
                coordinate_producer = CoordinateProducer()

                # Enviar coordenadas a través de Kafka
                coordinate_producer.send_coordinates(coordinates_json)

if __name__ == "__main__":
    main()
