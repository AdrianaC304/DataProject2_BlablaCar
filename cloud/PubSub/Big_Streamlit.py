import streamlit as st
import folium
from streamlit_folium import folium_static
from google.cloud import bigquery
import random

# Configura la credencial de Google Cloud para acceder a BigQuery
# Asegúrate de haber configurado tu credencial antes de ejecutar el script
# Más información: https://cloud.google.com/docs/authentication/getting-started
client = bigquery.Client()

# Función para leer datos de BigQuery
def leer_datos_bigquery(tabla):
    query = f"SELECT longitud, latitud, ruta FROM {tabla}"
    return client.query(query).to_dataframe()


def crear_mapa_folium(datos_personas, datos_coches):
    # Renombrar columnas para ambos conjuntos de datos
    datos_personas.rename(columns={'longitud': 'lon', 'latitud': 'lat'}, inplace=True)
    datos_coches.rename(columns={'longitud': 'lon', 'latitud': 'lat'}, inplace=True)

    # Coordenadas del centro del mapa
    center_coordinates = [
        (datos_personas['lat'].mean() + datos_coches['lat'].mean()) / 2,
        (datos_personas['lon'].mean() + datos_coches['lon'].mean()) / 2
    ]

    # Configuración del tamaño del mapa
    map_width, map_height = 2000, 1200

    # Crear un mapa de Folium
    mapa_folium = folium.Map(location=center_coordinates, zoom_start=5, control_scale=True, width=map_width, height=map_height)

    # Obtener la lista de rutas únicas para personas y coches
    rutas_personas = datos_personas['ruta'].unique()
    rutas_coches = datos_coches['ruta'].unique()

    # Colores aleatorios para rutas de personas y coches
    colores_rutas_personas = {ruta: f"#{random.randint(0, 0xFFFFFF):06x}" for ruta in rutas_personas}
    colores_rutas_coches = {ruta: f"#{random.randint(0, 0xFFFFFF):06x}" for ruta in rutas_coches}

    # Agregar puntos y rutas para personas
    for ruta in rutas_personas:
        ruta_data = datos_personas[datos_personas['ruta'] == ruta]
        color_ruta = colores_rutas_personas[ruta]

        for _, row in ruta_data.iterrows():
            folium.Marker(
                location=[row['lat'], row['lon']],
                popup=f"Coordenadas Personas: ({row['lat']}, {row['lon']})",
                icon=folium.Icon(color=color_ruta)
            ).add_to(mapa_folium)

        # Unir los puntos para formar una ruta con color específico
        folium.PolyLine(locations=ruta_data[['lat', 'lon']].values, color=color_ruta).add_to(mapa_folium)

    # Agregar puntos y rutas para coches
    for ruta in rutas_coches:
        ruta_data = datos_coches[datos_coches['ruta'] == ruta]
        color_ruta = colores_rutas_coches[ruta]

        for _, row in ruta_data.iterrows():
            folium.Marker(
                location=[row['lat'], row['lon']],
                popup=f"Coordenadas Coches: ({row['lat']}, {row['lon']})",
                icon=folium.Icon(color=color_ruta)
            ).add_to(mapa_folium)

        # Unir los puntos para formar una ruta con color específico
        folium.PolyLine(locations=ruta_data[['lat', 'lon']].values, color=color_ruta).add_to(mapa_folium)

    return mapa_folium

if __name__ == "__main__":
    # Nombre de la tabla en BigQuery que quieres leer
    nombre_tabla_personas = "woven-justice-411714.ejemplo.personas"
    nombre_tabla_coches = "woven-justice-411714.ejemplo.coches"

    # Lee los datos de BigQuery y crea el mapa
    datos_personas = leer_datos_bigquery(nombre_tabla_personas)
    datos_coches = leer_datos_bigquery(nombre_tabla_coches)

    mapa_folium = crear_mapa_folium(datos_personas, datos_coches)

    # Muestra la tabla en Streamlit
    st.title("Datos de BigQuery en Streamlit de rutas de Personas")
    st.dataframe(datos_personas)

    st.title("Datos de BigQuery en Streamlit de rutas de Coches")
    st.dataframe(datos_coches)

    # Muestra el mapa en Streamlit
    st.title("Rutas en Mapa desde BigQuery con Folium")
    folium_static(mapa_folium)
