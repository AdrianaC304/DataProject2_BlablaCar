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
        "geo": "39.49693-0.38688",
        "coches": [
            {
                "coche_id_message": "1fd328a1-90fa-4057-b6ae-1ef5fff1f762",
                "coche_id": 1,
                "coche_index_msg": 5,
                "coche_geo": "39.49693-0.38688",
                "coche_latitud": 39.49693,
                "coche_longitud": -0.38688,
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
                "user_geo_fin": "39.49693-0.38688",
                "user_latitud_inicio": 39.49597,
                "user_longitud_inicio": -0.38838,
                "user_latitud_destino": 39.49693,
                "user_longitud_destino": -0.38688
            }
        ],
        "fin_viaje": True
    },
    {
        "geo": "40.41678-3.70379",
        "coches": [
            {
                "coche_id_message": "2fd328a1-90fa-4057-b6ae-1ef5fff1f763",
                "coche_id": 2,
                "coche_index_msg": 6,
                "coche_geo": "40.41678-3.70379",
                "coche_latitud": 40.41678,
                "coche_longitud": -3.70379,
                "coche_datetime": "2024-01-31T14:31:43.111614Z",
                "coche_ruta": "sol-granvia.kml"
            }
        ],
        "usuarios": [
            {
                "user_id_message": "8d948746-cfc3-4b93-8653-a1565475ddfc",
                "user_id": 9991,
                "user_datetime": "2024-01-31T14:31:34.409989Z",
                "user_geo": "40.41572-3.70786",
                "user_geo_fin": "40.41678-3.70379",
                "user_latitud_inicio": 40.41572,
                "user_longitud_inicio": -3.70786,
                "user_latitud_destino": 40.41678,
                "user_longitud_destino": -3.70379
            }
        ],
        "fin_viaje": True
    },
    {
        "geo": "39.46928-0.37522",
        "coches": [
            {
                "coche_id_message": "3fd328a1-90fa-4057-b6ae-1ef5fff1f764",
                "coche_id": 3,
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
                "coche_id": 4,
                "coche_index_msg": 8,
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
        "geo": "39.46873-0.37212",
        "coches": [
            {
                "coche_id_message": "5fd328a1-90fa-4057-b6ae-1ef5fff1f766",
                "coche_id": 5,
                "coche_index_msg": 9,
                "coche_geo": "39.46873-0.37212",
                "coche_latitud": 39.46873,
                "coche_longitud": -0.37212,
                "coche_datetime": "2024-01-31T17:31:43.111614Z",
                "coche_ruta": "ciudad-arts-ciencias.kml"
            }
        ],
        "usuarios": [
            {
                "user_id_message": "2d948746-cfc3-4b93-8653-a1565475ddff",
                "user_id": 9994,
                "user_datetime": "2024-01-31T17:31:34.409989Z",
                "user_geo": "39.46947-0.37087",
                "user_geo_fin": "39.46873-0.37212",
                "user_latitud_inicio": 39.46947,
                "user_longitud_inicio": -0.37087,
                "user_latitud_destino": 39.46873,
                "user_longitud_destino": -0.37212
            }
        ],
        "fin_viaje": True
    },
    {
        "geo": "39.46462-0.37683",
        "coches": [
            {
                "coche_id_message": "6fd328a1-90fa-4057-b6ae-1ef5fff1f767",
                "coche_id": 6,
                "coche_index_msg": 10,
                "coche_geo": "39.46462-0.37683",
                "coche_latitud": 39.46462,
                "coche_longitud": -0.37683,
                "coche_datetime": "2024-01-31T18:31:43.111614Z",
                "coche_ruta": "barrio-carmen.kml"
            }
        ],
        "usuarios": [
            {
                "user_id_message": "3d948746-cfc3-4b93-8653-a1565475de00",
                "user_id": 9995,
                "user_datetime": "2024-01-31T18:31:34.409989Z",
                "user_geo": "39.46536-0.37558",
                "user_geo_fin": "39.46462-0.37683",
                "user_latitud_inicio": 39.46536,
                "user_longitud_inicio": -0.37558,
                "user_latitud_destino": 39.46462,
                "user_longitud_destino": -0.37683
            }
        ],
        "fin_viaje": True
    },
    {
        "geo": "39.46994-0.37468",
        "coches": [
            {
                "coche_id_message": "7fd328a1-90fa-4057-b6ae-1ef5fff1f768",
                "coche_id": 7,
                "coche_index_msg": 11,
                "coche_geo": "39.46994-0.37468",
                "coche_latitud": 39.46994,
                "coche_longitud": -0.37468,
                "coche_datetime": "2024-01-31T19:31:43.111614Z",
                "coche_ruta": "bioparc-turia.kml"
            }
        ],
        "usuarios": [
            {
                "user_id_message": "4d948746-cfc3-4b93-8653-a1565475de01",
                "user_id": 9996,
                "user_datetime": "2024-01-31T19:31:34.409989Z",
                "user_geo": "39.47069-0.37343",
                "user_geo_fin": "39.46994-0.37468",
                "user_latitud_inicio": 39.47069,
                "user_longitud_inicio": -0.37343,
                "user_latitud_destino": 39.46994,
                "user_longitud_destino": -0.37468
            }
        ],
        "fin_viaje": True
    },
    {
        "geo": "39.46825-0.38083",
        "coches": [
            {
                "coche_id_message": "8fd328a1-90fa-4057-b6ae-1ef5fff1f769",
                "coche_id": 8,
                "coche_index_msg": 12,
                "coche_geo": "39.46825-0.38083",
                "coche_latitud": 39.46825,
                "coche_longitud": -0.38083,
                "coche_datetime": "2024-01-31T20:31:43.111614Z",
                "coche_ruta": "malvarrosa-palau-musica.kml"
            }
        ],
        "usuarios": [
            {
                "user_id_message": "5d948746-cfc3-4b93-8653-a1565475de02",
                "user_id": 9997,
                "user_datetime": "2024-01-31T20:31:34.409989Z",
                "user_geo": "39.46899-0.38208",
                "user_geo_fin": "39.46825-0.38083",
                "user_latitud_inicio": 39.46899,
                "user_longitud_inicio": -0.38208,
                "user_latitud_destino": 39.46825,
                "user_longitud_destino": -0.38083
            }
        ],
        "fin_viaje": True
    },
    {
        "geo": "39.46714-0.37642",
        "coches": [
            {
                "coche_id_message": "9fd328a1-90fa-4057-b6ae-1ef5fff1f770",
                "coche_id": 9,
                "coche_index_msg": 13,
                "coche_geo": "39.46714-0.37642",
                "coche_latitud": 39.46714,
                "coche_longitud": -0.37642,
                "coche_datetime": "2024-01-31T21:31:43.111614Z",
                "coche_ruta": "alameda-catedral.kml"
            }
        ],
        "usuarios": [
            {
                "user_id_message": "6d948746-cfc3-4b93-8653-a1565475de03",
                "user_id": 9998,
                "user_datetime": "2024-01-31T21:31:34.409989Z",
                "user_geo": "39.46788-0.37767",
                "user_geo_fin": "39.46714-0.37642",
                "user_latitud_inicio": 39.46788,
                "user_longitud_inicio": -0.37767,
                "user_latitud_destino": 39.46714,
                "user_longitud_destino": -0.37642
            }
        ],
        "fin_viaje": True
    },
    {
        "geo": "39.46568-0.37495",
        "coches": [
            {
                "coche_id_message": "10fd328a1-90fa-4057-b6ae-1ef5fff1f771",
                "coche_id": 10,
                "coche_index_msg": 14,
                "coche_geo": "39.46568-0.37495",
                "coche_latitud": 39.46568,
                "coche_longitud": -0.37495,
                "coche_datetime": "2024-01-31T22:31:43.111614Z",
                "coche_ruta": "mestalla-puente-nuevo.kml"
            }
        ],
        "usuarios": [
            {
                "user_id_message": "7d948746-cfc3-4b93-8653-a1565475de04",
                "user_id": 9999,
                "user_datetime": "2024-01-31T22:31:34.409989Z",
                "user_geo": "39.46643-0.3762",
                "user_geo_fin": "39.46568-0.37495",
                "user_latitud_inicio": 39.46643,
                "user_longitud_inicio": -0.3762,
                "user_latitud_destino": 39.46568,
                "user_longitud_destino": -0.37495
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
