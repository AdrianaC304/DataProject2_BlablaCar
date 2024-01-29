# Import Python Libraries
import logging
import apache_beam as beam
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib


# Variables
project_id = "woven-justice-411714"
subscription_name = "edema_mda-sub"
bq_dataset = "ejemplo"
bq_table = "edem"
#bucket_name = "edem"
#
import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions

def decode_message(msg):

    output = msg.decode('utf-8')

    logging.info("New PubSub Message: %s", output)

    return json.loads(output)


# Pipeline configuración
#streaming ->> todos los workers imposrten las librerias que tenemos al principio

def run():
    with beam.Pipeline(options=PipelineOptions(streaming=True, save_main_session=True)) as p:
        (
            p 
            | "ReadFromPubSub" >> beam.io.ReadFromPubSub(subscription=f'projects/{project_id}/subscriptions/{subscription_name}')
            | "decode msg" >> beam.Map(decode_message)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table = f"{project_id}:{bq_dataset}.{bq_table}", # Required Format: PROJECT_ID:DATASET.TABLE
                schema='nombre:STRING', # Required Format: field:TYPE
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


