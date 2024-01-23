import pandas as pd
from google.cloud import bigquery
from geopy.distance import geodesic

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

# Filtrar las coordenadas en las que coinciden coches y personas a menos de 200 metros
coincidencias = []

for index, row in datos_combinados.iterrows():
    coord_coche = (row['latitud_x'], row['longitud_x'])
    coord_persona = (row['latitud_y'], row['longitud_y'])

    distancia = geodesic(coord_coche, coord_persona).meters

    if distancia < 200:
        coincidencias.append(row[['latitud_x', 'longitud_x', 'ruta']])

# Convertir la lista de coincidencias a un DataFrame
coincidencias_df = pd.DataFrame(coincidencias, columns=['latitud_x', 'longitud_x', 'ruta'])

# Mostrar las coordenadas en las que coinciden coches y personas a menos de 200 metros
print("Coordenadas en las que coinciden coches y personas a menos de 200 metros:")
print(coincidencias_df)
