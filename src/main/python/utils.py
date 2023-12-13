import folium
import pandas as pd
import geopandas as gpd
from folium import plugins
from folium.features import DivIcon

def add_marker_to_map(df_pd,m,lat_col,long_col,tooltip=None,popup=None,color="blue"):
  for _, row in df_pd.iterrows():
    folium.Marker(
      location = [row[lat_col],row[long_col]],
      tooltip = tooltip,
      popup = popup(row),
      icon=folium.Icon(color=color,icon="info-sign")
    ).add_to(m)
def add_polygon_to_map(df_pd,m,wkt_column,opacity=0.01,line_weight=0.01,color=None): 
  
    df_pd["polygon"] = gpd.GeoSeries.from_wkt(df_pd[wkt_column])
    geo_data = gpd.GeoDataFrame(df_pd,geometry="polygon")
    for _,row in  geo_data.iterrows():
      sim_geo = gpd.GeoSeries(row["polygon"]).simplify(tolerance=0.0001)
      geo_j = sim_geo.to_json()
      geo_j= folium.GeoJson(data=geo_j,
                               style_function=lambda x: {'fillColor': color,
                                                        'fillOpacity': opacity,
                                                        'color':color,
                                                            "weight": line_weight}
                           )
      geo_j.add_to(m)
def add_circle_to_map(df_pd,m,lat_col,long_col,tooltip=None,popup=None,color="blue"):
  for _, row in df_pd.iterrows():
    folium.CircleMarker(
      location = [row[lat_col],row[long_col]],
      radius=5,
      color=color,
      popup = popup(row)
    ).add_to(m)          
    

def add_marker_to_map(df_pd,m,lat_col,long_col,tooltip=None,popup=None,color="blue"):
  for _, row in df_pd.iterrows():
    folium.Marker(
      location = [row[lat_col],row[long_col]],
      tooltip = tooltip,
      popup = popup(row),
      icon=folium.Icon(color=color,icon="info-sign")
    ).add_to(m)
def add_polygon_to_map(df_pd,m,wkt_column,opacity=0.01,line_weight=0.01,color=None): 
  
    df_pd["polygon"] = gpd.GeoSeries.from_wkt(df_pd[wkt_column])
    geo_data = gpd.GeoDataFrame(df_pd,geometry="polygon")
    for _,row in  geo_data.iterrows():
      sim_geo = gpd.GeoSeries(row["polygon"]).simplify(tolerance=0.0001)
      geo_j = sim_geo.to_json()
      geo_j= folium.GeoJson(data=geo_j,
                               style_function=lambda x: {'fillColor': color,
                                                        'fillOpacity': opacity,
                                                        'color':color,
                                                            "weight": line_weight}
                           )
      geo_j.add_to(m)
def add_circle_to_map(df_pd,m,lat_col,long_col,color="blue"):
  for _, row in df_pd.iterrows():
    folium.CircleMarker(
      location = [row[lat_col],row[long_col]],
      radius=5,
      color=color
    ).add_to(m)