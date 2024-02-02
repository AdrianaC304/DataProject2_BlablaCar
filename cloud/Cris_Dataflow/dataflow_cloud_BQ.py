import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import CoGroupByKey
import json
from apache_beam.io.gcp.internal.clients import bigquery


################config################################

options = PipelineOptions(
    streaming=True,
    runner='DataflowRunner',
    experiments='enable_streaming_engine,use_beam_bq_sink'
)

##################################### Adri ##################################################
suscripcion_coche = 'projects/woven-justice-411714/subscriptions/blablacar_coches-sub'
suscripcion_usuario = 'projects/woven-justice-411714/subscriptions/blablacar_usuarios-sub'
project_id = 'woven-justice-411714'
bucket_name = "woven-justice-411714"
table_id= 'asignaciones'
dataset_id= 'ejemplo'


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







################################## Funciones para BQ #################################################
######################################################################################################

# Define una función para convertir el diccionario a una cadena JSON
def convert_to_json(element):
    return json.dumps(element)


############### Pipeline #################################
# Define el esquema de la tabla
new_table_schema = bigquery.TableSchema()
new_table_fields = [
    bigquery.TableFieldSchema(name='geo', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_id_message', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_id', type='INTEGER', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_index_msg', type='INTEGER', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_geo', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_latitud', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_longitud', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_datetime', type='DATETIME', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='coche_ruta', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_id_message', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_id', type='INTEGER', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_datetime', type='DATETIME', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_geo', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_geo_fin', type='STRING', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_latitud_inicio', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_longitud_inicio', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_latitud_destino', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='user_longitud_destino', type='FLOAT', mode='NULLABLE'),
    bigquery.TableFieldSchema(name='inicio_viaje', type='BOOLEAN', mode='NULLABLE')
]
new_table_schema.fields.extend(new_table_fields)



# Crear el pipeline
with beam.Pipeline(options=PipelineOptions(streaming=True)) as p:
    
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



    # Seleccionar campos específicos de joined_data_inicio
    selected_fields_inicio = (
        joined_data_inicio
        | "Seleccionar_Campos_inicio" >> beam.Map(lambda element: {
            'geo': element['geo'],
            'coche_id_message': element['coches'][0]['coche_id_message'],
            'coche_id': element['coches'][0]['coche_id'],
            'coche_index_msg': element['coches'][0]['coche_index_msg'],
            'coche_geo': element['coches'][0]['coche_geo'],
            'coche_latitud': element['coches'][0]['coche_latitud'],
            'coche_longitud': element['coches'][0]['coche_longitud'],
            'coche_datetime': element['coches'][0]['coche_datetime'],
            'coche_ruta': element['coches'][0]['coche_ruta'],
            'user_id_message': element['usuarios'][0]['user_id_message'],
            'user_id': element['usuarios'][0]['user_id'],
            'user_datetime': element['usuarios'][0]['user_datetime'],
            'user_geo': element['usuarios'][0]['user_geo'],
            'user_geo_fin': element['usuarios'][0]['user_geo_fin'],
            'user_latitud_inicio': element['usuarios'][0]['user_latitud_inicio'],
            'user_longitud_inicio': element['usuarios'][0]['user_longitud_inicio'],
            'user_latitud_destino': element['usuarios'][0]['user_latitud_destino'],
            'user_longitud_destino': element['usuarios'][0]['user_longitud_destino'],
            'inicio_viaje': element['inicio_viaje'],
        })
    )

    # Escribir los resultados en BigQuery
    selected_fields_inicio | "Convertir_a_JSON_inicio" >> beam.Map(convert_to_json)  | "Escribir_en_BigQuery_inicio" >> beam.io.WriteToBigQuery(
        table=table_id,
        dataset=dataset_id,
        project=project_id,
        schema=new_table_schema,  # Utiliza el esquema definido anteriormente
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
    )