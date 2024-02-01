import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import CoGroupByKey
import json

################config################################

options = PipelineOptions(
    streaming=True,
    runner='DataflowRunner',
    experiments='enable_streaming_engine,use_beam_bq_sink'
)

########################################## Cris #############################################
# Definir suscripciones y otros detalles si es necesario
#suscripcion_coche = 'projects/dataflow-1-411618/subscriptions/coches_stream-sub'
#suscripcion_usuario = 'projects/dataflow-1-411618/subscriptions/usuarios_stream-sub'

##################################### Adri ##################################################
suscripcion_coche = 'projects/woven-justice-411714/subscriptions/blablacar_coches-sub'
suscripcion_usuario = 'projects/woven-justice-411714/subscriptions/blablacar_usuarios-sub'
project_id = 'woven-justice-411714'
bucket_name = "woven-justice-411714"



################################## Funciones #######################################
   
# Recibe datos
class DecodeMessage(beam.DoFn):
    def process(self, element):
        output = element.decode('utf-8')
        json_data = json.loads(output)
        return [json_data]

# Función para extraer la clave 'user_geo' de cada elemento para el inicio del viaje
def extract_geo_user(element):
    geo = element.get('user_geo', None)
    return (geo, element)

# Función para extraer la clave 'user_geo_fin' de cada elemento para el fin del viaje
def extract_geo_fin(element):
    geo = element.get('user_geo_fin', None)
    return (geo, element)

# Función para extraer la clave 'coche_geo' de cada elemento
def extract_geo_coche(element):
    geo = element.get('coche_geo', None)
    return (geo, element)

# Función para filtrar casos coincidentes y no coincidentes para el inicio del viaje
class FilterCoincidentCases_inicio(beam.DoFn):
    def process(self, element):
        geo_key, messages = element
        coches = messages['coches']
        usuarios = messages['usuarios']

        if coches and usuarios:
            yield {'geo': geo_key, 'coches': coches, 'usuarios': usuarios, 'inicio_viaje': True}
        else:
            yield {'geo': geo_key, 'coches': coches, 'usuarios': usuarios, 'inicio_viaje': False}

# Función para filtrar casos coincidentes y no coincidentes para el fin del viaje
class FilterCoincidentCases_fin(beam.DoFn):
    def process(self, element):
        geo_key, messages = element
        coches = messages['coches']
        usuarios = messages['usuarios']

        if coches and usuarios:
            yield {'geo': geo_key, 'coches': coches, 'usuarios': usuarios, 'fin_viaje': True}
        else:
            yield {'geo': geo_key, 'coches': coches, 'usuarios': usuarios, 'fin_viaje': False}




############### Pipeline #################################

# Crear el pipeline
with beam.Pipeline(options=PipelineOptions(
        streaming=True,
        save_main_session=True,
        job_name = "edem-test1",
        project=project_id,
        runner="DataflowRunner",
        #donde guarda los archivos
        temp_location=f"gs://{bucket_name}/tmp",
       staging_location=f"gs://{bucket_name}/staging",
        region="europe-west4"
        )) as p:
    
    # Coches
    coches_data = (
        p
        | "Coche_LeerDesdePubSub" >> beam.io.ReadFromPubSub(subscription=suscripcion_coche)
        | "Coche_decodificar_msg" >> beam.ParDo(DecodeMessage())
        | "Coche_Extraer_Clave_geo" >> beam.Map(extract_geo_coche)
        | "Coche_ventana_5_minutos" >> beam.WindowInto(beam.window.FixedWindows(500))
    )

    # Usuarios
    usuarios_data = (
        p
        | "Usuario_LeerDesdePubSub" >> beam.io.ReadFromPubSub(subscription=suscripcion_usuario)
        | "Usuario_decodificar_msg" >> beam.ParDo(DecodeMessage())
        | "Usuario_ventana_5_minutos" >> beam.WindowInto(beam.window.FixedWindows(500))
    )

    # Derivar dos flujos distintos para inicio y fin del viaje
    usuarios_data_inicio = (
        usuarios_data
        | "Usuario_Extraer_Clave_geo_inicio" >> beam.Map(extract_geo_user)
        | "Etiquetar_inicio_viaje" >> beam.Map(lambda x: (x[0], (x[1], 'inicio')))
    )

    usuarios_data_fin = (
        usuarios_data
        | "Usuario_Extraer_Clave_geo_fin" >> beam.Map(extract_geo_fin)
        | "Etiquetar_fin_viaje" >> beam.Map(lambda x: (x[0], (x[1], 'fin')))
    )

    # Realizar un CoGroupByKey en base al campo 'geo'_inicio
    joined_data_inicio = (
        {'coches': coches_data, 'usuarios': usuarios_data_inicio}
        | "Merge_Mensajes_por_geo" >> CoGroupByKey()
        | "Filtrar_Casos_Coincidentes" >> beam.ParDo(FilterCoincidentCases_inicio())
        | "Filtrar_Solo_Coincidentes" >> beam.Filter(lambda element: element['inicio_viaje'])
        | "Imprimir_Resultados_inic" >> beam.Map(lambda element: print(element))
    )

    # Realizar un CoGroupByKey en base al campo 'geo'_fin
    joined_data_fin = (
        {'coches': coches_data, 'usuarios': usuarios_data_fin}
        | "Merge_Mensajes_por_geo_fin" >> CoGroupByKey()
        | "Filtrar_Casos_Coincidentes_fin" >> beam.ParDo(FilterCoincidentCases_fin())
        | "Filtrar_Solo_Coincidentes_fin" >> beam.Filter(lambda element: element['fin_viaje'])
        | "Imprimir_Resultados_fin" >> beam.Map(lambda element: print(element))
    )
