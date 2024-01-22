import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
import json
from datetime import datetime


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
new_table_schema_personas = bigquery.TableSchema()
new_table_fields_personas = [
    bigquery.TableFieldSchema(name='id_message', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='persona_id', type='INTEGER', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='index_msg', type='INTEGER', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='latitud', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='longitud', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='datetime', type='DATETIME', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='ruta', type='STRING', mode='NULLABLE')
]
new_table_schema_personas.fields.extend(new_table_fields_personas)

with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
    #coches:
    data = (
        p
        | "LeerDesdePubSub2" >> beam.io.ReadFromPubSub(subscription='projects/woven-justice-411714/subscriptions/blablacar_persona-sub')
        | "decodificar_msg2" >> beam.ParDo(DecodeMessage())
        | "escribir2" >> beam.io.WriteToBigQuery(
            table="woven-justice-411714:ejemplo.personas",
            schema=new_table_schema_personas,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
