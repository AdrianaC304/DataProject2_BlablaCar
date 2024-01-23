################# Asignación del conductor al coche #################

import pandas as pd
from google.cloud import bigquery

# Configura la credencial de Google Cloud para acceder a BigQuery
# Asegúrate de haber configurado tu credencial antes de ejecutar el script
# Más información: https://cloud.google.com/docs/authentication/getting-started
client = bigquery.Client()

# Nombre de las tablas en BigQuery
tabla_personas = "woven-justice-411714.ejemplo.personas"
tabla_coches = "woven-justice-411714.ejemplo.coches"

# Función para leer datos de BigQuery
def leer_datos_bigquery(tabla):
    query = f"SELECT latitud, longitud, ruta FROM {tabla}"
    return client.query(query).to_dataframe()

# Leer datos de coches y personas
datos_coches = leer_datos_bigquery(tabla_coches)
datos_personas = leer_datos_bigquery(tabla_personas)

# Realizar un merge en función de latitud, longitud y ruta
datos_combinados = pd.merge(datos_coches, datos_personas, on=['latitud', 'longitud', 'ruta'])

# Filtrar las coordenadas en las que coinciden coches y personas
coincidencias = datos_combinados[['latitud', 'longitud', 'ruta']].drop_duplicates()

# Mostrar las coordenadas en las que coinciden coches y personas
print("Coordenadas en las que coinciden coches y personas:")
print(coincidencias)
