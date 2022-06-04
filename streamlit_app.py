from collections import namedtuple
import altair as alt
import math
import pandas as pd
import streamlit as st
import pyodbc
import pydeck as pdk

# Initialize SQL connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
        + st.secrets["server"]
        + ";DATABASE="
        + st.secrets["database"]
        + ";UID="
        + st.secrets["username"]
        + ";PWD="
        + st.secrets["password"]
    )

conn = init_connection()

# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 secs.
@st.experimental_memo(ttl=10)
def run_query(query):
    with conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

counts = run_query("SELECT top 10 * from twitter_api.HashtagAggregations;")
tweets = run_query("SELECT top 10 * from twitter_api.TweetText")


############ Display page content ############
"""
# Big Data Project Dashboard (Twitter Use Case)
### Weigl-Pollack & Pscheidl ###
"""

# Print latest tweets
for row in tweets:
    st.write(f"*Tag:* {row[2]}, *Tweet:* {row[0]}")


# Print results.
for row in counts:
    st.write(f"Tag {row[0]} had {row[1]} tweets in the last minute!")

# map
MAP_POINT_DATA = pd.DataFrame(data={
    'lng': [-74.0060152, 16.3725042, -122.419906, 116.3912757, -46.6333824, 13.3888599, 2.320041, -0.1276474], 
    'lat': [40.741895, 48.2083537, 37.7790262, 39.906217, -23.5506507, 52.5170365, 48.8588897, 51.5073219]
})

# lng,lat
# -74.0060152,40.741895 New York
# 16.3725042,48.2083537 Vienna
# -122.419906,37.7790262 San Francisco
# 116.3912757,39.906217 Beijing
# -46.6333824,-23.5506507 Sao Paulo
# 13.3888599,52.5170365 Berlin
# 2.320041, 48.8588897 Paris
# -0.1276474, 51.5073219 London

layer = pdk.Layer(
    "HexagonLayer",
    MAP_POINT_DATA,
    get_position="[lng, lat]",
    auto_highlight=True,
    elevation_scale=50,
    pickable=True,
    elevation_range=[0, 3000],
    extruded=True,
    coverage=1,
)
# # Set the viewport location
# view_state = pdk.ViewState(
#     longitude=-1.415, latitude=52.2323, zoom=6, min_zoom=5, max_zoom=15, pitch=40.5, bearing=-27.36
# )
# Combined all of it and render a viewport
r = pdk.Deck(
    map_style="mapbox://styles/mapbox/light-v9",
    layers=[layer],
    #initial_view_state=view_state,
    tooltip={"html": "<b>Elevation Value:</b>", "style": {"color": "white"}},
)
st.pydeck_chart(r)


############ End page content ############