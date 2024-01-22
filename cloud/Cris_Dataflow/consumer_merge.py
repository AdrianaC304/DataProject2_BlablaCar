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
        print(f"JSON guardado en BigQuery: {json_data}")
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
######################################### Transfromaciones ########################################################
###################################################################################################################


with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
    data_topic1 = (
        p
        | "LeerDesdePubSub" >> beam.io.ReadFromPubSub(subscription='projects/woven-justice-411714/subscriptions/blablacar_DataProject2-sub')
        | "decodificar_msg" >> beam.ParDo(DecodeMessage())
    
    )
    data_topic2 = (
        p
        | "LeerDesdePubSub2" >> beam.io.ReadFromPubSub(subscription='projects/woven-justice-411714/subscriptions/blablacar_personas-sub')
        | "decodificar_msg2" >> beam.ParDo(DecodeMessage())
    
    )

 # Asigna el campo común como clave para ambas PCollections
    keyed_data_topic1 = data_topic1 | "KeyByCampoComun1" >> beam.Map(lambda x: (x['campo_comun'], x))
    keyed_data_topic2 = data_topic2 | "KeyByCampoComun2" >> beam.Map(lambda x: (x['campo_comun'], x))

    # Utiliza CoGroupByKey para combinar los datos por el campo común
    merged_data = ({'data_topic1': keyed_data_topic1, 'data_topic2': keyed_data_topic2}
                   | "MergeTopics" >> beam.CoGroupByKey()
                   | "Flatten" >> beam.Map(lambda element: element[1]))

    # Realiza cualquier asignación adicional o transformación necesaria
    transformed_data = (
        merged_data
        | "RealizarTransformacion" >> beam.Map(lambda element: print(element) or element)
    )