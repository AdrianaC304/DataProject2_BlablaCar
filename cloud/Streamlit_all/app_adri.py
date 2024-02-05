import streamlit as st
import pandas as pd
from google.cloud import bigquery
import folium
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static
import time
from folium import Marker
import base64
import random
import streamlit as st
import folium
from folium.plugins import HeatMap
from google.cloud import bigquery
import streamlit as st
import pandas as pd
import plotly.express as px

project_id_adri = 'woven-justice-411714'
topic_name_adri = 'blablacar_DataProject2'
tabla_name_coches = 'woven-justice-411714.ejemplo.coches'
tabla_name_usuarios= 'woven-justice-411714.ejemplo.usuarios'




################################################### Configuramos la página para que ocupe la anchura completa del navegador ################################

st.set_page_config(layout="wide", page_title="¡Bienvenido/a a Streamlit!", page_icon="🚗")
logo_url="https://user-images.githubusercontent.com/8149019/166203710-737d477f-c325-4417-8518-7b378918d1f1.png"
st.image(logo_url, width=40)


st.title("DASHBOARD BLABLACAR VALENCIA")
st.write("Bienvenido al dashboard de BlaBlaCar para la ciudad de Valencia. Aquí podrás visualizar y analizar datos relacionados con viajes compartidos en la ciudad.")
# Creamos dos pestañas para las distintas visualizaciones que necesitamos
tab1, tab2, tab3 = st.tabs(["En directo", "Métricas", "Datos"])

################################################################################################################################################################
client = bigquery.Client()
 
tabla_name= 'dataflow-1-411618.blablacar.asignaciones'
def leer_datos_bigquery(tabla):
    # Agrega comillas inversas alrededor del nombre de la tabla
    query = f"SELECT * FROM {tabla_name} ORDER BY coche_index_msg ASC "  
    return client.query(query).to_dataframe()


def coches_totales(tabla_coches_totales):
    query = f"SELECT COUNT(DISTINCT coche_id) as coches_totales FROM `{tabla_name_coches}`"

def coches_dia(tabla_coches_dia):
    query = f"SELECT DATE(datetime) as fecha, COUNT(DISTINCT coche_id) as coches_dia FROM `{tabla_name_coches}` GROUP BY fecha"

# Función para crear un mapa de Folium con la ruta y colores diferentes por coche_id
def crear_mapa_folium(datos, ruta_seleccionada=None):
    datos.rename(columns={'coche_longitud': 'lon', 'coche_latitud': 'lat'}, inplace=True)

    # Filtrar datos por ruta seleccionada
    if ruta_seleccionada:
        datos = datos[datos['coche_ruta'] == ruta_seleccionada]

    # Calcular el centro promedio de las coordenadas de las rutas seleccionadas
    if not datos.empty:
        center_coordinates = [datos['lat'].mean(), datos['lon'].mean()]
    else:
        # Si no hay datos, establecer un centro predeterminado
        center_coordinates = [39.4699, -0.3763]

    # Configuración del tamaño del mapa
    map_width, map_height = 2000, 1200

    # Crear un mapa de Folium con un estilo simple y gris
    mapa_folium = folium.Map(location=center_coordinates, zoom_start=5, control_scale=True, width=map_width, height=map_height,  tiles='CartoDB positron')

    # Generar colores aleatorios para cada coche_id
    colores = {coche_id: "#{:06x}".format(random.randint(0, 0xFFFFFF)) for coche_id in datos['coche_id'].unique()}

    # Crear diccionario para almacenar polilíneas por coche_id
    polilineas_por_coche = {}

    # Agregar puntos a la ruta con colores diferentes por coche_id
    for _, row in datos.iterrows():
        color = colores[row['coche_id']]
        folium.Marker(location=[row['lat'], row['lon']],
                      popup=f"Coche ID: {row['coche_id']}, Ruta: {row['coche_ruta']}, Coordenadas: ({row['lat']}, {row['lon']})",
                      icon=folium.Icon(color=color)).add_to(mapa_folium)

        # Crear o actualizar polilínea para el coche_id
        if row['coche_id'] not in polilineas_por_coche:
            polilineas_por_coche[row['coche_id']] = []
        polilineas_por_coche[row['coche_id']].append([row['lat'], row['lon']])

    # Agregar polilíneas al mapa
    for coche_id, coordenadas in polilineas_por_coche.items():
        color = colores[coche_id]
        # Evitar que la última coordenada se conecte con la primera
        folium.PolyLine(locations=coordenadas, color=color).add_to(mapa_folium)

    return mapa_folium



################################################################################################


with tab3:

    # Nombre de la tabla en BigQuery que quieres leer
    nombre_tabla = tabla_name_coches 
    #nombre_tabla_usuarios = tabla_name_usuarios # Reemplaza con tu información real
    # Lee los datos de BigQuery
    datos = leer_datos_bigquery(nombre_tabla)
    #datos_usuarios = leer_datos_bigquery(nombre_tabla_usuarios)
    
    # Obtener la lista de rutas únicas
    rutas_unicas = datos['coche_ruta'].unique()
    # Agregar un slicer (selectbox) para seleccionar la ruta
    ruta_seleccionada = st.selectbox("Selecciona una ruta:", rutas_unicas)
    # Crea el mapa y la tabla filtrados por la ruta seleccionada
    mapa_folium = crear_mapa_folium(datos, ruta_seleccionada)

    # Muestra la tabla en Streamlit
    st.title("Datos de Rutas coches")
    st.dataframe(datos[datos['coche_ruta'] == ruta_seleccionada])
   # st.title("Datos de usuarios")
   # st.dataframe(datos_usuarios)

    # Muestra el mapa en Streamlit
    st.title("Ruta en Folium")
    folium_static(mapa_folium)



with tab2:

    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    with col1:
            st.subheader("Mapa de calor")
            # Configuración de BigQuery
            project_id = 'woven-justice-411714'
            dataset_id = 'ejemplo'
            table_id = 'asignaciones'
            
            datos = leer_datos_bigquery(tabla_name)
            if not datos.empty:
                center_coordinates = [datos['coche_latitud'].mean(), datos['coche_longitud'].mean()]
            else:
              center_coordinates = [0, 0]

            mymap = folium.Map(location=center_coordinates, zoom_start=13)

            # Agrega el mapa de calor con las coordenadas de latitud y longitud
            heat_data = [[row['coche_latitud'], row['coche_longitud']] for index, row in datos.iterrows()]
            HeatMap(heat_data).add_to(mymap)

            # Muestra el mapa en Streamlit
            folium_static(mymap)
            

    with col2:
           st.subheader("Grafico de viajes asigandos")
            # Lee los datos de BigQuery
            # Lee los datos de BigQuery
           datos = leer_datos_bigquery(tabla_name)
            # Filtra los datos para aquellos con inicio_viaje=True y fin_viaje=True
           viajes_completos = datos[(datos['inicio_viaje'] == True) ]
            # Cuenta el número de registros para cada coche
           conteo_viajes_completos = viajes_completos['coche_id'].value_counts()
            # Crea un gráfico de barras con Plotly Express
           fig = px.bar(x=conteo_viajes_completos.index, y=conteo_viajes_completos.values, labels={'x': 'ID del Coche', 'y': 'Número de Viajes Completos'},title='Número de Viajes Completos por Coche')
           # Muestra el gráfico en Streamlit
           st.plotly_chart(fig)

    with col3:
            st.subheader("Pasajeros/Día")
            #Insertar código
           # st.subheader("Valoración media")
            #Insertar código
           # st.subheader("Pasajeros/Coche")
            #Insertar código
            #st.subheader("Duración media")
            #Insertar código


################################################################################################
    
with tab1:

    # Configura tus detalles de proyecto, conjunto de datos y tabla
    project_id = 'dataflow-1-411618'
    dataset_id = 'blablacar'
    table_id = 'asignaciones'
    #tabla_name = 'dataflow-1-411618.blablacar.asignaciones'

    # Recupera los datos de BigQuery
    df = leer_datos_bigquery(tabla_name)

    # Muestra el DataFrame en Streamlit
    st.write("Datos de BigQuery de asignaciones:")
    st.write(df)

    st.title("Mapa interactivo")
    # Establece las coordenadas del centro de Valencia, España
    valencia_center_coordinates = [39.4699, -0.3763]
    # Contenedor para el mapa
    map_container = st.empty()
    # Crea un mapa centrado en Valencia
    mymap = folium.Map(location=valencia_center_coordinates, zoom_start=13)
    route_coordinates = []

    while True:
        for i in range(len(df)):
            latitud = float(df.loc[i, 'coche_latitud'])
            longitud = float(df.loc[i, 'coche_longitud'])

            icon = folium.Icon(color='red', icon='car', prefix='fa')
            marker = folium.Marker(location=[latitud, longitud], popup=f"Vehicle ID: {df.loc[i, 'coche_id']}", icon=icon).add_to(mymap)

            # Añade la nueva coordenada a la ruta
            route_coordinates.append([latitud, longitud])

            # Añade la nueva línea a la ruta
            if len(route_coordinates) > 1:
                folium.PolyLine(locations=route_coordinates[-2:], color='red').add_to(mymap)

            # Convierte el mapa de Folium a HTML y muestra el HTML directamente en Streamlit
            map_html = f'<iframe width="1000" height="500" src="data:text/html;base64,{base64.b64encode(mymap._repr_html_().encode()).decode()}" frameborder="0" allowfullscreen="true"></iframe>'
            map_container.markdown(map_html, unsafe_allow_html=True)

            time.sleep(1)
