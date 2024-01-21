import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
import json

# Definir la función decode_message
def decode_message(msg):
    # Lógica para decodificar el mensaje y cargarlo como JSON
    output = msg.decode('utf-8')
    json_data = json.loads(output)
    print(f"JSON guardado en BigQuery: {json_data}")
    return json_data

# Definir el esquema de BigQuery
table_schema = bigquery.TableSchema()
field_schema = bigquery.TableFieldSchema(name='nombre', type='STRING', mode='NULLABLE')
table_schema.fields.append(field_schema)

with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
    data = (
        p
        | "LeerDesdePubSub" >> beam.io.ReadFromPubSub(subscription='projects/dataflow-1-411618/subscriptions/new_topic-sub')
        | "decodificar_msg" >> beam.Map(decode_message)
        | "escribir" >> WriteToBigQuery(
              table="dataflow-1-411618:data_test.tabla",
              schema=table_schema,
              create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
              write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
