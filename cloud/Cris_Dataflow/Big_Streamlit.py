import streamlit as st
import folium
from streamlit_folium import folium_static
from google.cloud import bigquery
import random
#################################################### Adriana ###################################################

#roject_id = 'woven-justice-411714'
#topic_name= 'blablacar_DataProject2'

################################################################################################################

#################################################### Cris ######################################################

project_id = 'dataflow-1-411618'
topic_name= 'coches'
tabla_name = 'dataflow-1-411618.blablacar.rutas'

################################################################################################################





client = bigquery.Client()

def leer_datos_bigquery(tabla):
    # Agrega comillas inversas alrededor del nombre de la tabla
    query = f"SELECT coche_id, index_msg, longitud, latitud, datetime, ruta FROM `{tabla}` ORDER BY datetime DESC LIMIT 100"  
    return client.query(query).to_dataframe()

# Función para crear un mapa de Folium con la ruta y colores diferentes por coche_id
def crear_mapa_folium(datos):
    datos.rename(columns={'longitud': 'lon', 'latitud': 'lat'}, inplace=True)

    # Coordenadas del centro del mapa
    center_coordinates = [datos['lat'].mean(), datos['lon'].mean()]

    # Configuración del tamaño del mapa
    map_width, map_height = 2000, 1200

    # Crear un mapa de Folium
    mapa_folium = folium.Map(location=center_coordinates, zoom_start=5, control_scale=True, width=map_width, height=map_height)

    # Generar colores aleatorios para cada coche_id
    colores = {coche_id: "#{:06x}".format(random.randint(0, 0xFFFFFF)) for coche_id in datos['coche_id'].unique()}

    # Crear diccionario para almacenar polilíneas por coche_id
    polilineas_por_coche = {}

    # Agregar puntos a la ruta con colores diferentes por coche_id
    for _, row in datos.iterrows():
        color = colores[row['coche_id']]
        folium.Marker(location=[row['lat'], row['lon']],
                      popup=f"Coche ID: {row['coche_id']}, Ruta: {row['ruta']}, Coordenadas: ({row['lat']}, {row['lon']})",
                      icon=folium.Icon(color=color)).add_to(mapa_folium)

        # Crear polilínea si aún no existe para el coche_id
        if row['coche_id'] not in polilineas_por_coche:
            polilineas_por_coche[row['coche_id']] = folium.PolyLine(locations=[], color=color)

        # Agregar coordenadas a la polilínea correspondiente al coche_id
        polilineas_por_coche[row['coche_id']].add_child(folium.Marker(location=[row['lat'], row['lon']]))

    # Agregar polilíneas al mapa
    for coche_id, polilinea in polilineas_por_coche.items():
        polilinea.add_to(mapa_folium)

    return mapa_folium

if __name__ == "__main__":
    # Nombre de la tabla en BigQuery que quieres leer
    nombre_tabla = tabla_name  # Reemplaza con tu información real

    # Lee los datos de BigQuery y crea el mapa
    datos = leer_datos_bigquery(nombre_tabla)
    mapa_folium = crear_mapa_folium(datos)

    # Muestra la tabla en Streamlit
    st.title("Datos de BigQuery en Streamlit")
    st.dataframe(datos)

    # Muestra el mapa en Streamlit
    st.title("Ruta en Mapa desde BigQuery con Folium")
    folium_static(mapa_folium)