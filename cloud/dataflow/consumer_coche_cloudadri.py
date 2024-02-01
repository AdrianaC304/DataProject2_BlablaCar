
##########################################################################

##{"coche_id_message": "4754c777-cc23-41c7-a912-39956f40c1d5",
#"coche_id': 1, 
#"coche_index_msg": 1, 
#"geo'": "39.4934-0.3914", 
#"coche_latitud': 39.4934, 
#"coche_longitud": -0.3914, 
#"datetime": '"2024-01-30T21:47:18.758751", 
#"coche_ruta": "benicalap-alboraya.kml"}

###########################################################################


################################ Script para escribir en Big Query la información de los coches ####################################

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
import json
from datetime import datetime
import logging

#################################################### Adriana ###################################################
project_id = 'woven-justice-411714'
#topic_name= 'blablacar_coche'
topic_name = 'test_dataflow'
table_name = "woven-justice-411714:ejemplo.coches"
#suscripcion ='projects/woven-justice-411714/subscriptions/blablacar_coche-sub'
suscripcion = 'projects/woven-justice-411714/subscriptions/test_dataflow-sub'
bucket_name = "woven-justice-411714"


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
##################################################################################################


# Recibe datos
def decode_message(msg):
    # Lógica para decodificar el mensaje y cargarlo como JSON
    output = msg.decode('utf-8')
    json_data = json.loads(output)
    print(f"JSON guardado en BigQuery: {json_data}")
    return json_data

# Obtiene la hora actual en formato UTC
current_time_utc = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

class DecodeMessage(beam.DoFn):
    def process(self, element):
        output = element.decode('utf-8')
        json_data = json.loads(output)
        print(f"JSON guardado en BigQuery: {json_data}")
        return [json_data]

# Nueva definición del esquema para BigQuery
new_table_schema = bigquery.TableSchema()
new_table_fields = [
    bigquery.TableFieldSchema(name='coche_id_message', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_id', type='INTEGER', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_index_msg', type='INTEGER', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='geo', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_latitud', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_longitud', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='datetime', type='datetime', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='ruta', type='STRING', mode='NULLABLE')
]
new_table_schema.fields.extend(new_table_fields)


def run():
    with beam.Pipeline(options=PipelineOptions(
        streaming=True,
        # save_main_session=True
        job_name = "edem-t",
        project=project_id,
        runner="DataflowRunner",
        #donde guarda los archivos
        temp_location=f"gs://{bucket_name}/tmp",
        staging_location=f"gs://{bucket_name}/staging",
        region="europe-west1"
    )) as p:
        (
            p
            | "Leer PubSub" >> beam.io.ReadFromPubSub(subscription= suscripcion)
            | "Decode msg" >> beam.Map(decode_message)
            | "Agregar fecha hora" >> beam.Map(lambda elem: {**elem, 'datetime': current_time_utc})
            | "Escribir" >> beam.io.WriteToBigQuery(
                table = table_name,
                schema= new_table_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    logging.info("The process started")

    # Run Process
    run()


