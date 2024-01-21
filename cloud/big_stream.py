import streamlit as st

from google.cloud import bigquery

# Configura la credencial de Google Cloud para acceder a BigQuery
# Asegúrate de haber configurado tu credencial antes de ejecutar el script
# Más información: https://cloud.google.com/docs/authentication/getting-started
client = bigquery.Client()

# Función para leer datos de BigQuery
def leer_datos_bigquery(tabla):
    query = f"SELECT * FROM {tabla}"
    df = client.query(query).to_dataframe()
    return df

# Nombre de la tabla en BigQuery que quieres leer
nombre_tabla = "ejemplo.conductores"

# Lee los datos de BigQuery
datos = leer_datos_bigquery(nombre_tabla)

# Muestra los datos en Streamlit
st.title("Datos de BigQuery en Streamlit")
st.dataframe(datos)
