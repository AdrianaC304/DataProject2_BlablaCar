from google.cloud import bigquery

# Configura las credenciales de Google Cloud y el proyecto
# INSTALAR BIBLIOTECA 'google-cloud-bigquery' EN EL ENTORNO
# pip install google-cloud-bigquery

client = bigquery.Client(project='dataproject-blablacar')

# Detalles de la tabla en BigQuery
dataset_id = 'dataset_st'
table_id = 'tabla_st'
tabla_bigquery = f'{client.project}.{dataset_id}.{table_id}'

# Datos a insertar

datos = [
  {
    "geo": "39.4934-0.3914",
    "coches": [
      {
        "coche_id_message": "1fd328a1-90fa-4057-b6ae-1ef5fff1f762",
        "coche_id": 1,
        "coche_index_msg": 5,
        "coche_geo": "39.4934-0.3914",
        "coche_latitud": 39.4934,
        "coche_longitud": -0.3914,
        "coche_datetime": "2024-01-31T13:31:43.111614Z",
        "coche_ruta": "benicalap-alboraya.kml"
      }
    ],
    "usuarios": [
      {
        "user_id_message": "7d948746-cfc3-4b93-8653-a1565475ddfb",
        "user_id": 9990,
        "user_datetime": "2024-01-31T13:31:34.409989Z",
        "user_geo": "39.49597-0.38838",
        "user_geo_fin": "39.4934-0.3914",
        "user_latitud_inicio": 39.49597,
        "user_longitud_inicio": -0.38838,
        "user_latitud_destino": 39.49693,
        "user_longitud_destino": -0.38688
      }
    ],
    "fin_viaje": True
  },
  {
    "geo": "39.46928-0.37522",
    "coches": [
      {
        "coche_id_message": "3fd328a1-90fa-4057-b6ae-1ef5fff1f764",
        "coche_id": 1,
        "coche_index_msg": 7,
        "coche_geo": "39.46928-0.37522",
        "coche_latitud": 39.46928,
        "coche_longitud": -0.37522,
        "coche_datetime": "2024-01-31T15:31:43.111614Z",
        "coche_ruta": "plaza-ayuntamiento.kml"
      }
    ],
    "usuarios": [
      {
        "user_id_message": "9d948746-cfc3-4b93-8653-a1565475ddfd",
        "user_id": 9992,
        "user_datetime": "2024-01-31T15:31:34.409989Z",
        "user_geo": "39.47003-0.37397",
        "user_geo_fin": "39.46928-0.37522",
        "user_latitud_inicio": 39.47003,
        "user_longitud_inicio": -0.37397,
        "user_latitud_destino": 39.46928,
        "user_longitud_destino": -0.37522
      }
    ],
    "fin_viaje": True
  },
  {
    "geo": "39.46786-0.37585",
    "coches": [
      {
        "coche_id_message": "4fd328a1-90fa-4057-b6ae-1ef5fff1f765",
        "coche_id": 1,
        "coche_index_msg": 9,
        "coche_geo": "39.46786-0.37585",
        "coche_latitud": 39.46786,
        "coche_longitud": -0.37585,
        "coche_datetime": "2024-01-31T16:31:43.111614Z",
        "coche_ruta": "turia-pont-del-real.kml"
      }
    ],
    "usuarios": [
      {
        "user_id_message": "1d948746-cfc3-4b93-8653-a1565475ddfe",
        "user_id": 9993,
        "user_datetime": "2024-01-31T16:31:34.409989Z",
        "user_geo": "39.46851-0.37778",
        "user_geo_fin": "39.46786-0.37585",
        "user_latitud_inicio": 39.46851,
        "user_longitud_inicio": -0.37778,
        "user_latitud_destino": 39.46786,
        "user_longitud_destino": -0.37585
      }
    ],
    "fin_viaje": True
  },
  {
    "geo": "39.47001-0.37628",
    "coches": [
      {
        "coche_id_message": "5fd328a1-90fa-4057-b6ae-1ef5fff1f766",
        "coche_id": 1,
        "coche_index_msg": 11,
        "coche_geo": "39.47001-0.37628",
        "coche_latitud": 39.47001,
        "coche_longitud": -0.37628,
        "coche_datetime": "2024-01-31T17:31:43.111614Z",
        "coche_ruta": "valencia-centro.kml"
      }
    ],
    "usuarios": [
      {
        "user_id_message": "2d948746-cfc3-4b93-8653-a1565475ddff",
        "user_id": 9994,
        "user_datetime": "2024-01-31T17:31:34.409989Z",
        "user_geo": "39.46974-0.37736",
        "user_geo_fin": "39.47001-0.37628",
        "user_latitud_inicio": 39.46974,
        "user_longitud_inicio": -0.37736,
        "user_latitud_destino": 39.47001,
        "user_longitud_destino": -0.37628
      }
    ],
    "fin_viaje": True
  }
]




# Convertir la estructura de datos para la inserción

datos_a_insertar = []

for elemento in datos:
    coches = elemento.get("coches", [])
    usuarios = elemento.get("usuarios", [])

    datos_a_insertar.append({
        "geo": elemento["geo"],
        "fin_viaje": elemento["fin_viaje"],
        "coches": coches,
        "usuarios": usuarios,
    })

# Insertar los datos en BigQuery

try:
    errors = client.insert_rows_json(tabla_bigquery, datos_a_insertar)
    if errors:
        print(f"Errores durante la inserción: {errors}")
    else:
        print("Datos insertados con éxito.")
except Exception as e:
    print(f"Error al insertar datos: {str(e)}")
