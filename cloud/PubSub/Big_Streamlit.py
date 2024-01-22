import streamlit as st
import folium
from streamlit_folium import folium_static
from google.cloud import bigquery

# Configura la credencial de Google Cloud para acceder a BigQuery
# Asegúrate de haber configurado tu credencial antes de ejecutar el script
# Más información: https://cloud.google.com/docs/authentication/getting-started
client = bigquery.Client()

# Función para leer datos de BigQuery
def leer_datos_bigquery(tabla):
    query = f"SELECT longitud, latitud FROM {tabla}"
    return client.query(query).to_dataframe()

# Función para crear un mapa de Folium con la ruta
def crear_mapa_folium(datos):
    datos.rename(columns={'longitud': 'lon', 'latitud': 'lat'}, inplace=True)

    # Coordenadas del centro del mapa
    center_coordinates = [datos['lat'].mean(), datos['lon'].mean()]

    # Configuración del tamaño del mapa
    map_width, map_height = 2000, 1200

    # Crear un mapa de Folium
    mapa_folium = folium.Map(location=center_coordinates, zoom_start=5, control_scale=True, width=map_width, height=map_height)

    # Agregar puntos a la ruta
    for _, row in datos.iterrows():
        folium.Marker(location=[row['lat'], row['lon']], popup=f"Coordenadas: ({row['lat']}, {row['lon']})").add_to(mapa_folium)

    # Unir los puntos para formar una ruta
    folium.PolyLine(locations=datos[['lat', 'lon']].values, color='blue').add_to(mapa_folium)

    return mapa_folium

if __name__ == "__main__":
    # Nombre de la tabla en BigQuery que quieres leer
    nombre_tabla = "woven-justice-411714.ejemplo.coches"  # Reemplaza con tu información real

    # Lee los datos de BigQuery y crea el mapa
    datos = leer_datos_bigquery(nombre_tabla)
    mapa_folium = crear_mapa_folium(datos)

    # Muestra la tabla en Streamlit
    st.title("Datos de BigQuery en Streamlit")
    st.dataframe(datos)

    # Muestra el mapa en Streamlit
    st.title("Ruta en Mapa desde BigQuery con Folium")
    folium_static(mapa_folium)
