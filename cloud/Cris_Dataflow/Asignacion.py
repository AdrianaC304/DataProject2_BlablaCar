import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
import json
from datetime import datetime



######################################################################################################



# Recibe datos
def decode_message(msg):
    # Lógica para decodificar el mensaje y cargarlo como JSON
    output = msg.decode('utf-8')
    json_data = json.loads(output)
    #print(f"JSON guardado en BigQuery: {json_data}")
    return json_data

# Obtiene la hora actual en formato UTC
current_time_utc = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

class DecodeMessage(beam.DoFn):
    def process(self, element):
        output = element.decode('utf-8')
        json_data = json.loads(output)
        #print(f"JSON guardado en BigQuery: {json_data}")
        return [json_data]

# Nueva definición del esquema para BigQuery
new_table_schema = bigquery.TableSchema()
new_table_fields = [
    bigquery.TableFieldSchema(name='id_message', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_id', type='INTEGER', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='index_msg', type='INTEGER', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='latitud', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='longitud', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='datetime', type='DATETIME', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='ruta', type='STRING', mode='NULLABLE')
]
new_table_schema.fields.extend(new_table_fields)





###################################################################################################################
######################################### Transformaciones ########################################################
###################################################################################################################


class JoinPersonCar(beam.DoFn):
    def process(self, element):
        id, data = element
        cars = data['cars']
        persons = data['persons']

        print(f"Processing ID {id}")
        print("Cars:", cars)
        print("Persons:", persons)

        for car in cars:
            for person in persons:
                # Comparar longitud y latitud (ajustar según necesidades)
                if abs(car['longitud'] - person['longitud']) < 0.001 and abs(car['latitud'] - person['latitud']) < 0.001:
                    yield f"Coincidencia encontrada: Car ID {car['coche_id']} y Person ID {person['coche_id']} en el mismo lugar."

from apache_beam.transforms.trigger import AfterProcessingTime

def run():
    with beam.Pipeline() as p:
        # Leer datos de coches
        cars_data = (
            p | "ReadCarsData" >> beam.io.ReadFromPubSub(subscription='projects/woven-justice-411714/subscriptions/blablacar_DataProject2-sub')
              | "DecodeCarsMessage" >> beam.Map(decode_message)  # Implementa tu lógica de decodificación
              | "KeyByCarId" >> beam.Map(lambda car: (car['coche_id'], car))
              | "WindowCars" >> beam.WindowInto(beam.window.FixedWindows(10))  # Ajusta el tamaño de la ventana según sea necesario
        )

        # Leer datos de personas
        persons_data = (
            p | "ReadPersonsData" >> beam.io.ReadFromPubSub(subscription='projects/woven-justice-411714/subscriptions/blablacar_personas-sub')
                | "DecodePersonsMessage" >> beam.Map(decode_message)  # Implementa tu lógica de decodificación
                | "KeyByPersonId" >> beam.Map(lambda person: (person['coche_id'], person))
                | "WindowPersons" >> beam.WindowInto(beam.window.FixedWindows(10))  # Ajusta el tamaño de la ventana según sea necesario
        )

     # Agrupar datos por ID
        grouped_data = (
            {'cars': cars_data, 'persons': persons_data}
            | "MergeData" >> beam.CoGroupByKey()
        )

        # Comparar puntos en común
        result = (
            grouped_data
            | "JoinData" >> beam.ParDo(JoinPersonCar())
        )

        # Puedes realizar cualquier acción adicional con los resultados, como imprimirlos o almacenarlos.
        result | "PrintResults" >> beam.Map(print)

if __name__ == "__main__":
    run()
