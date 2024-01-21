# Importa las bibliotecas necesarias
import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions


# Esta función decode_message se define para decodificar los mensajes recibidos del tema de Pub/Sub.

def decode_message(msg):
    output = msg.decode('utf-8')

    return json.loads(output)

# Importa las bibliotecas necesarias
import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions

def decode_message(msg):
    output = msg.decode('utf-8')
    return json.loads(output)

def extract_matricula(element):
    return element.get('Matricula', '')

# Crea el pipeline de Apache Beam
with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:

    # Lee desde un tema de Pub/Sub
    (
        p 
        | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription='projects/woven-justice-411714/subscriptions/edema_mda-sub')
        | "decode msg" >> beam.Map(decode_message)
        | "PairWithMatricula" >> beam.Map(lambda element: (extract_matricula(element), element))
        | "GroupByMatricula" >> beam.GroupByKey()
        | "write" >> beam.io.WriteToBigQuery(
            table="woven-justice-411714:ejemplo.conductores_agrupados",
            schema="Matricula:STRING, Datos:STRING",  # Puedes ajustar el esquema según tus necesidades
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )


