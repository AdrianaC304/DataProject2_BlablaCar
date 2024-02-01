import streamlit as st
import pandas as pd
import altair as alt
import plotly.express as px
#from google.cloud import bigquery
import os
import json
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static

#CONEXION A BIGQUERY. 

# client = bigquery.Client.from_service_account_json('/Users/jackeline/Documents/dataproject-blablacar-1e4c2dfcf976.json')

st.title("DASHBOARD BLABLACAR VALENCIA")
st.write("Bienvenido al dashboard de BlaBlaCar para la ciudad de Valencia. Aquí podrás visualizar y analizar datos relacionados con viajes compartidos en la ciudad.")

def main():
    
    json_file_path = "/Users/jackeline/Documents/GitHub/DataProject2_BlablaCar/visualizacion_streamlit/datos.json"
    
    # Verificar si el archivo JSON existe
    if not os.path.exists(json_file_path):
        st.warning("El archivo 'datos.json' no se encuentra en la ruta especificada.")
        return

    # Leer el contenido del archivo JSON
    with open(json_file_path, 'r') as file:
        json_content = file.read()

    # Cargar el JSON en un diccionario
    data = json.loads(json_content)

    # Crear un DataFrame con pandas
    df = pd.DataFrame(data['asignacion'])

    # Mostrar el DataFrame en Streamlit
    st.write("**DataFrame Asignaciones:**")
    st.write(df)
    

    # Crear un mapa centrado en la primera ubicación
    map_center = [data["asignacion"][0]["coche_latitud"], data["asignacion"][0]["coche_longitud"]]
    mymap = folium.Map(location=map_center, zoom_start=10)
    
    # Agrupar los marcadores en clústeres
    marker_cluster = MarkerCluster().add_to(mymap)

    for asignacion in data["asignacion"]:
        folium.Marker(
            location=[asignacion["coche_latitud"], asignacion["coche_longitud"]],
            popup=f"Coche {asignacion['coche_id']}",
            icon=folium.Icon(color='blue')
        ).add_to(marker_cluster)
    
    st.title("Mapa de Ubicaciones")
    
    folium_static(mymap)
    
if __name__ == "__main__":
    main()
