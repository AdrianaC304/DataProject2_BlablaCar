import streamlit as st
from streamlit_folium import folium_static
import folium

def main():
    # Título de la aplicación
    st.title("Mapa Interactivo")

    # Coordenadas de los puntos de la ruta
    coordinates = [
        [39.4465493, -0.3688011],
        [39.44811, -0.36404],
        [39.44972, -0.36588],
        [39.45286, -0.35212],
        [39.45286, -0.35187],
        [39.45287,  -0.35179]
    ]

    # Coordenadas del centro del mapa
    center_coordinates = coordinates[0]

    # Crear un mapa de Folium
    map = folium.Map(location=center_coordinates, zoom_start=15)

    # Agregar puntos a la ruta
    for coord in coordinates:
        folium.Marker(location=coord, popup=f"Coordenadas: {coord}").add_to(map)

    # Unir los puntos para formar una ruta
    folium.PolyLine(locations=coordinates, color='blue').add_to(map)

    # Mostrar el mapa en Streamlit
    folium_static(map)

if __name__ == "__main__":
    main()





