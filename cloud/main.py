# Importa las bibliotecas necesarias
import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions

def decode_message(msg):
    output = msg.decode('utf-8')

    return json.loads(output)

    
# Crea el pipeline de Apache Beam
with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:

    # Lee desde un tema de Pub/Sub
    (
        p 
        | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription='projects/woven-justice-411714/subscriptions/edema_mda-sub')
        | "decode msg" >> beam.Map(decode_message)
        | "write" >> beam.io.WriteToBigQuery(
            table="woven-justice-411714:ejemplo.conductores",
            schema="Longitud:FLOAT, Latitud:FLOAT, Nombre conductor:STRING, Matricula:STRING",
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
        
       )