import streamlit as st
from google.cloud import bigquery
from folium.plugins import MarkerCluster
from streamlit_folium import folium_static
import time
from folium import Marker
import base64
import random
import folium
from folium.plugins import HeatMap
import pandas as pd
import plotly.express as px
import streamlit as st
import hashlib
import os
import altair as alt
from dotenv import load_dotenv


tabla_name_coche = 'woven-justice-411714.ejemplo.coches'
tabla_name_usuarios= 'woven-justice-411714.ejemplo.usuarios'
tabla_name = 'dataflow-1-411618.blablacar.asignaciones'
tabla_name_vehiculos = 'dataflow-1-411618.blablacar.vehiculos'
tabla_name_vehiculo_info = 'dataflow-1-411618.blablacar.usuarios'



################################################### Configuramos la página para que ocupe la anchura completa del navegador ################################

st.set_page_config(layout="wide", page_title="Dashboard BlaBlaCar", page_icon="🚗",menu_items={
        'Get Help': 'http://www.policia.es/',
        'Report a bug': "http://www.plagaskil.com/",
        'About': "# ¿Qué haces mirando aquí? Este es el Data Project de Adriana, Cristian, Jackeline, Jesús y Mar."
    })


# Cargar variables de entorno desde un archivo .env
load_dotenv()

# Obtener la contraseña almacenada en la variable de entorno
contrasena_guardada = os.getenv("CONTRASENA_HASH")

# Solicitar al usuario que ingrese la contraseña
contrasena_ingresada = st.text_input("Contraseña:", type="password")

# Hash de la contraseña ingresada
hash_ingresado = hashlib.sha256(contrasena_ingresada.encode()).hexdigest()

# Verificar si el hash de la contraseña ingresada es igual al hash almacenado
if hash_ingresado == contrasena_guardada:
    
    logo_url="https://user-images.githubusercontent.com/8149019/166203710-737d477f-c325-4417-8518-7b378918d1f1.png"
    st.image(logo_url, width=40)
    # Creamos dos pestañas para las distintas visualizaciones que necesitamos
    tab1, tab2, tab3 , tab4 = st.tabs(["En directo", "Métricas", "Coches", "Gráficos"])

    ################################################################################################################################################################
    client = bigquery.Client()
    

    ################################################################################################################################################################
    ################################################################ Funciones SQL #################################################################################
    ################################################################################################################################################################


    def leer_datos_bigquery(tabla):
        # Agrega comillas inversas alrededor del nombre de la tabla
        query = f"SELECT * FROM {tabla} ORDER BY coche_index_msg ASC "  
        return client.query(query).to_dataframe()

    def leer_datos_bigquery_filtrado(tabla):
        # Agrega comillas inversas alrededor del nombre de la tabla
        query = f"SELECT coche_id,coche_datetime,user_id,inicio_viaje,fin_viaje FROM {tabla} ORDER BY coche_datetime ASC "  
        return client.query(query).to_dataframe()


    def leer_datos_bigquery_coche(tabla_coche):
        # Agrega comillas inversas alrededor del nombre de la tabla
        query = f"SELECT * FROM {tabla_coche}"  
        return client.query(query).to_dataframe()

    def coches_totales(tabla_coches_totales):
        query = f"SELECT COUNT(DISTINCT coche_id) as coches_totales FROM `{tabla_coches_totales}`"
        return client.query(query).to_dataframe()

    def coches_dia(tabla_coches_dia):
        query = f"SELECT DATE(coche_datetime) as fecha, COUNT(DISTINCT coche_id) as coches_dia FROM `{tabla_coches_dia}` GROUP BY fecha"
        return client.query(query).to_dataframe()


    def coches_total_viaje(tabla):
        query = f"SELECT coche_id, COUNT(user_id) as total_viajes FROM `{tabla}` GROUP BY coche_id;"
        return client.query(query).to_dataframe()



    def join_asignacion_vehiculo(tabla_asignaciones, tabla_vehiculos):
        query = f"""
        SELECT
            coches.coche_id,
            COUNT(asignaciones.user_id) as total_viajes,
            coches.license_plate,
            coches.owner_name,
            coches.car_brand,
            coches.car_model
        FROM `{tabla_vehiculos}` as coches
        LEFT JOIN `{tabla_asignaciones}` as asignaciones
        ON coches.coche_id = asignaciones.coche_id
        GROUP BY coches.coche_id, coches.car_brand, coches.owner_name, coches.car_model, coches.license_plate
        """
        return client.query(query).to_dataframe()

    def calcular_precio(tabla_asignaciones):
        query = f"""
        SELECT
        asignaciones.user_id,
        COUNT(asignaciones.coche_id) as total_viajes,
        SUM(CASE WHEN asignaciones.inicio_viaje THEN asignaciones.coche_index_msg ELSE 0 END) as sum_inicio,
        SUM(CASE WHEN asignaciones.fin_viaje THEN asignaciones.coche_index_msg ELSE 0 END) as sum_fin,
        SUM(CASE WHEN asignaciones.fin_viaje THEN asignaciones.coche_index_msg ELSE 0 END) - 
        SUM(CASE WHEN asignaciones.inicio_viaje THEN asignaciones.coche_index_msg ELSE 0 END) as km_distancia_recorrida,
        ROUND((SUM(CASE WHEN asignaciones.fin_viaje THEN asignaciones.coche_index_msg ELSE 0 END) - 
        SUM(CASE WHEN asignaciones.inicio_viaje THEN asignaciones.coche_index_msg ELSE 0 END)) * 0.35, 1) as importe_euros
        FROM `{tabla_asignaciones}` as asignaciones
        WHERE DATE(asignaciones.user_datetime) = CURRENT_DATE()
        GROUP BY asignaciones.user_id
        order by importe_euros;
        """
        return client.query(query).to_dataframe()
    
    # Function to load data from BigQuery
    def load_data_from_bigquery():
    # Authenticate to Google Cloud
    # You can skip this step if you're running the script on Google Colab or a GCP environment
    # Otherwise, make sure you have set up authentication properly
    # See https://cloud.google.com/docs/authentication/getting-started for more details
            client = bigquery.Client()

    # Query your data from BigQuery
            query = """
            SELECT
            car_brand,
            COUNT(*) AS count
            FROM
                dataflow-1-411618.blablacar.vehiculos
            GROUP BY
            car_brand
            ORDER BY
            COUNT(*) ASC
            """

        # Execute the query
            query_job = client.query(query)

        # Convert the results to a DataFrame
            results = query_job.result().to_dataframe()

            return results

    # Function to create ascending brand evolution chart
    def brand_evolution_chart(data):
        chart = alt.Chart(data).mark_line().encode(
            x='car_brand',
            y='count'
        ).properties(
            width=600,
            height=400
        )
        st.altair_chart(chart, use_container_width=True)


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


    with tab2:

        col1, col2, col3 = st.columns([3, 1, 1])
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
            # Streamlit layout
                st.subheader("Evolución marcas de coche")

            # Load data from BigQuery
                data = load_data_from_bigquery()

            # LAYING OUT THE MIDDLE SECTION OF THE APP WITH THE CHART
                st.write("**Evolución Ascendente de Marcas de Coche**")
                brand_evolution_chart(data)




##########################################################################################################
        

    with tab4:

        col1, col2, col3 = st.columns([3, 1, 1])
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



    ################################################################################################

    with tab3:

        st.subheader("Información de los vehiculos")
        datos_vehiculos = leer_datos_bigquery_coche(tabla_name_vehiculos)
        st.dataframe(datos_vehiculos)
        st.subheader("Información de los usuarios") 
        datos_usu = leer_datos_bigquery_coche(tabla_name_vehiculo_info)
        st.dataframe(datos_usu)


    ##########
        datos_vehiculos = coches_totales(tabla_name_vehiculos)
        # Extrae el valor que deseas mostrar en el KPI
        total_coches = datos_vehiculos['coches_totales'].values[0]
        # Muestra el KPI
        st.subheader("Total de vehículos")
        st.metric("Nº de vehículos", total_coches)
    #############
        
        st.subheader("Coches por día")
        datos_vehiculos_dia = coches_dia(tabla_name)
        st.dataframe(datos_vehiculos_dia)

        #st.subheader("Usuarios por coche")
        #datos_user_coche = coches_total_viaje(tabla_name)
        #st.dataframe(datos_user_coche)

        st.subheader("Usuarios por coche")
        datos_join = join_asignacion_vehiculo(tabla_name,tabla_name_vehiculos)
        st.dataframe(datos_join)


        st.subheader("Precio del usuario")
        datos_join = calcular_precio(tabla_name)
        st.dataframe(datos_join)

        datos = leer_datos_bigquery_coche(tabla_name_coche)
        rutas_unicas = datos['coche_id'].unique()
        # Agregar un slicer (selectbox) para seleccionar la ruta
        ruta_seleccionada = st.selectbox("Selecciona una ruta:", rutas_unicas)
        # Crea el mapa y la tabla filtrados por la ruta seleccionada
        mapa_folium = crear_mapa_folium(datos, ruta_seleccionada)
        # Muestra la tabla en Streamlit
        st.subheader("Datos de Rutas coches")
        st.dataframe(datos[datos['coche_id'] == ruta_seleccionada])
        #st.dataframe(datos)
        


################################################################################################
        

    with tab1:
        # Configura tus detalles de proyecto, conjunto de datos y tabla
        project_id = 'dataflow-1-411618'
        dataset_id = 'blablacar'
        table_id = 'asignaciones'

        df_filtrado = leer_datos_bigquery_filtrado(tabla_name)
        # Muestra el DataFrame en Streamlit
        #st.write("Datos de BigQuery de asignaciones:")
        #st.write(df_filtrado)

        # Recupera los datos de BigQuery
        df = leer_datos_bigquery(tabla_name)
        st.subheader("Mapa interactivo")
    
        # Establece las coordenadas del centro de Valencia, España
        valencia_center_coordinates = [39.4699, -0.3763]
    
        # Contenedor para el mapa
        map_container = st.empty()
        # Crea un mapa centrado en Valencia
        mymap = folium.Map(location=valencia_center_coordinates, zoom_start=13)
    
        car_route_coordinates = []
        user_route_coordinates = []

        while True:
            for i in range(len(df)):
                car_latitud = float(df.loc[i, 'coche_latitud'])
                car_longitud = float(df.loc[i, 'coche_longitud'])
                car_route_coordinates.append([car_latitud, car_longitud])
                
                icon_car = folium.Icon(color='red', icon='car', prefix='fa')
                marker_car = folium.Marker(location=[car_latitud, car_longitud], popup=f"Vehicle ID: {df.loc[i, 'coche_id']}", icon=icon_car).add_to(mymap)
                
                user_latitud = float(df.loc[i, 'user_latitud_destino'])
                user_longitud = float(df.loc[i, 'user_longitud_destino'])  
                user_route_coordinates.append([user_latitud, user_longitud])
                
                icon_user = folium.Icon(color='blue', icon='user', prefix='fa')
                marker_user = folium.Marker(location=[user_latitud, user_longitud], popup=f"User ID: {df.loc[i, 'user_id']}", icon=icon_user).add_to(mymap)

                # Añade las líneas de ruta después del bucle
                if len(car_route_coordinates) > 1:
                    folium.PolyLine(locations=car_route_coordinates[-2:], color='red').add_to(mymap)

                if len(user_route_coordinates) > 1:
                    folium.PolyLine(locations=user_route_coordinates[-2:], color='blue').add_to(mymap)
            

                # Convierte el mapa de Folium a HTML y muestra el HTML directamente en Streamlit
                map_html = f'<iframe width="1000" height="500" src="data:text/html;base64,{base64.b64encode(mymap._repr_html_().encode()).decode()}" frameborder="0" allowfullscreen="true"></iframe>'
                map_container.markdown(map_html, unsafe_allow_html=True)
                    
                time.sleep(1)

    ################################################################################################
else:
    st.write("Contraseña incorrecta, matao.")