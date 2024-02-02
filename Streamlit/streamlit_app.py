import streamlit as st
import pandas as pd
from google.cloud import bigquery
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static

# Función para obtener datos desde BigQuery
def get_bigquery_data(project_id, dataset_id, table_id):
    # Configura la autenticación de Google Cloud
    client = bigquery.Client(project=project_id)

    # Realiza la consulta a BigQuery
    query = f"""
        SELECT
          geo,
          coche.coche_id,
          coche.coche_datetime,
          coche.coche_latitud,
          coche.coche_longitud,
          coche.coche_ruta,
          usuario.user_id,
          usuario.user_datetime,
          usuario.user_latitud_inicio,
          usuario.user_longitud_inicio,
          usuario.user_latitud_destino,
          usuario.user_longitud_destino,
          fin_viaje
        FROM
          `{project_id}.{dataset_id}.{table_id}`,
          UNNEST(coches) AS coche,
          UNNEST(usuarios) AS usuario;
    """

    df = client.query(query).to_dataframe()
    return df

# Función para crear un mapa interactivo con Folium
def create_interactive_map(df):
    # Crea un mapa centrado en la primera ubicación
    m = folium.Map(location=[df['coche_latitud'].iloc[0], df['coche_longitud'].iloc[0]], zoom_start=12)

    # Agrega marcadores para cada fila en el DataFrame
    for _, row in df.iterrows():
        folium.Marker(
            location=[row['coche_latitud'], row['coche_longitud']],
            popup=f"Coche ID: {row['coche_id']}\nUsuario ID: {row['user_id']}",
            icon=folium.Icon(color='blue' if row['fin_viaje'] else 'green')
        ).add_to(m)

    return m

st.set_page_config(page_title="Visualización de Datos en Streamlit", page_icon="🚗")
st.title("DASHBOARD BLABLACAR VALENCIA")
st.write("Bienvenido al dashboard de BlaBlaCar para la ciudad de Valencia. Aquí podrás visualizar y analizar datos relacionados con viajes compartidos en la ciudad.")

def main():
    # Configura tus detalles de proyecto, conjunto de datos y tabla
    project_id = 'dataproject-blablacar'
    dataset_id = 'dataset_st'
    table_id = 'tabla_st'

    # Recupera los datos de BigQuery
    df = get_bigquery_data(project_id, dataset_id, table_id)
    
    # Crea el mapa interactivo
    st.write("Mapa interactivo:")
    folium_map = create_interactive_map(df)
    folium_static(folium_map)

    # Muestra el DataFrame en Streamlit
    st.write("Datos de BigQuery:")
    
    st.write(df)

if __name__ == "__main__":
    main()
