import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions


##################################################### Variables ##################################
project_id = "woven-justice-411714"
subscription_name = "blablacar_coche-sub"
bq_dataset = "ejemplo"
bq_table = "coches"
# Buenas prácticas debe ser unico porque si no pueden acceder 
bucket_name = "woven-justice-411714"
##################################################################################################

def decode_message(msg):

    output = msg.decode('utf-8')

    logging.info("New PubSub Message: %s", output)

    return json.loads(output)

def run():
    with beam.Pipeline(options=PipelineOptions(
        streaming=True,
        # save_main_session=True
        job_name = "edem-dataflowc",
        project=project_id,
        runner="DataflowRunner",
        #donde guarda los archivos
        temp_location=f"gs://{bucket_name}/tmp",
        staging_location=f"gs://{bucket_name}/staging",
        region="europe-west1"
    )) as p:
        (
            p
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name}')
            | "decode msg" >> beam.Map(decode_message)
            | "ESCRIBIR" >> beam.io.WriteToBigQuery(
                table = f"{project_id}:{bq_dataset}.{bq_table}", # Required Format: PROJECT_ID:DATASET.TABLE
                schema='coche_id_message:STRING, coche_id: INTEGER , coche_index_msg: INTEGER, geo:STTRING, coche_latitud:FLOAT, coche_longitud:FLOAT, datetime: DATETIME, coche_ruta: STRING', # Required Format: field:TYPE
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


        