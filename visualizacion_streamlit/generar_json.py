import json

# Define los datos como un diccionario
datos = {
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
}

# Convierte el diccionario a JSON
json_data = json.dumps(datos, indent=4)

# Guarda el JSON en un archivo
with open("datos.json", "w") as archivo:
    archivo.write(json_data)

print("JSON guardado en datos.json")
