################################ Script para escribir en Big Query la información de los coches ####################################

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
import json
from datetime import datetime

#################################################### Adriana ###################################################
#project_id = 'woven-justice-411714'
#topic_name= 'blablacar_DataProject2'
#table_name = "woven-justice-411714:ejemplo.coches"
#suscripcion ='projects/woven-justice-411714/subscriptions/blablacar_DataProject2-sub'
#################################################### Cris ######################################################
#project_id = 'dataflow-1-411618'
#topic_name= 'coches'
#table_name = 'dataflow-1-411618:blablacar.rutas'
#suscripcion = 'projects/dataflow-1-411618/subscriptions/coches'
################################################################################################################
#project_id = 'blablacar-412022'
#topic_name= 'coches'
#table_name = 'blablacar-412022.dataset.coches'
#suscripcion = 'projects/blablacar-412022/subscriptions/coches-sub'

#################################################### MAR ######################################################
project_id = 'blablacar-412417'
topic_name= 'blablacar'
table_name = 'blablacar-412417.blaablacar_database.tabla_blablacar'
suscripcion = 'projects/blablacar-412417/subscriptions/blablacar-sub'




# Recibe datos
def decode_message(msg):
    # Lógica para decodificar el mensaje y cargarlo como JSON
    output = msg.decode('utf-8')
    json_data = json.loads(output)
    print(f"JSON guardado en BigQuery: {json_data}")
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



