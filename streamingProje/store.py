import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.internal.clients import bigquery
import json

def decode_message(msg):
    # Lógica para decodificar el mensaje y cargarlo como JSON
    output = msg.decode('utf-8')
    json_data = json.loads(output)
    print(f"JSON guardado en BigQuery: {json_data}")
    return json_data

# Nueva definición del esquema para BigQuery
new_table_schema = bigquery.TableSchema()
new_table_fields = [
    bigquery.TableFieldSchema(name='index', type='INTEGER', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='latitud', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='longitud', type='FLOAT', mode='NULLABLE')
]
new_table_schema.fields.extend(new_table_fields)


with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
    data = (
        p
        | "LeerDesdePubSub" >> beam.io.ReadFromPubSub(subscription='projects/dataflow-1-411618/subscriptions/coches-sub')
        | "decodificar_msg" >> beam.Map(decode_message)
        | "escribir" >> WriteToBigQuery(
            table="dataflow-1-411618:blablacar.coordenadas",
            schema=new_table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
