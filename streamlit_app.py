#from collections import namedtuple
#import altair as alt
#import math
import pandas as pd
from sqlalchemy import column
import streamlit as st
import pyodbc
import pydeck as pdk
from streamlit_autorefresh import st_autorefresh
import pickle

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

def getTweets(tag, minutes):
    #tweets = run_query(f"SELECT TOP {count} * FROM twitter_api.TweetText WHERE Tag LIKE '{tag}' ORDER BY insertedAt DESC")
    tweets = run_query(f"select * from twitter_api.TweetText where DATEDIFF(second, insertedAt, GETDATE()) < {minutes*60} and Tag like '{tag}'")
    lst = []
    for row in tweets:
        lst.insert(len(lst), [row[2], row[0], row[1]])
        #lst.reverse()
    return pd.DataFrame(lst, columns=('Tag', 'Text', 'Time'))

def getTags():
    tags = run_query(f"select distinct Tag from twitter_api.TweetText order by Tag")
    lst = ['%']
    for row in tags:
        lst.insert(len(lst), row[0])
    return pd.DataFrame(lst)

def getTweetsPerMinute(map):
    map2 = map.copy(deep=False)
    i = 0
    while i<len(map2):
        tpm = run_query(f"select sum(count) as tpm from twitter_api.HashtagAggregations where datediff(second, LastUpdatedAt, getdate()) <= 60 and Tagname='{map2.iloc[i]['city']}' group by Tagname")
        if len(tpm) > 0:
            map2.loc[i, 'tweetsperminute'] = tpm[0][0]
            #print(tpm[0][0])
        i = i+1
    #print(map2)
    return map2

# map data
MAP_POINT_DATA = pd.DataFrame(
    [
       [-74.0060152,40.741895,'NewYork', 0],
       [16.3725042,48.2083537, 'Vienna', 0],
       [-122.419906,37.7790262, 'SanFrancisco', 0],
       [116.3912757,39.906217, 'Beijing', 0],
       [-46.6333824,-23.5506507, 'SaoPaulo', 0],
       [13.3888599,52.5170365, 'Berlin', 0],
       [2.320041, 48.8588897, 'Paris', 0],
       [-0.1276474, 51.5073219, 'London', 0],
       [36.81667, -1.28333, 'Nairobi', 0],
       [18.42322, -33.92584, 'CapeTown', 0],
       [100.489664708, 13.751330328, 'Bangkok', 0],
       [151.2114425, -33.863578, 'Sydney', 0]
    ],
    columns = ['lng', 'lat', 'city', 'tweetsperminute']
)

############ Display page content ############
count = st_autorefresh(interval=15000, key="refreshpage")
# refresh every 10 seconds to get actual data (no other option in streamlit)

"""
# Twitter Use Case Dashboard
#### Weigl-Pollack & Pscheidl ####
"""

MAP_POINT_DATA = getTweetsPerMinute(MAP_POINT_DATA)

"""
## Latest Tweets ##
"""
tweets_tag = st.selectbox(
    'Select City',
     getTags()
)

minutes = st.slider(
    'Display Tweets of the last ... minutes',
    min_value=0,
    max_value=15,
    value = 2
)

df_tweets = getTweets(tweets_tag, minutes)
table = st.empty()
table.dataframe(df_tweets)

'Showing tweets for ', tweets_tag


"""
## Tweet Map ##
"""
layers = [
    pdk.Layer(
        "ScatterplotLayer",
        data = MAP_POINT_DATA,
        pickable=True,
        get_position="[lng, lat]",
        get_color='[200, 30, 0, 160]',
        get_radius=200000,
    )
]

#Combine all of it and render a viewport
r = pdk.Deck(
    map_style="mapbox://styles/mapbox/light-v9",
    layers = layers,
    tooltip={"html": "<b>{city}</b><br/>Tweets per Minute: {tweetsperminute}", "style": {"color": "white"}},
)

st.pydeck_chart(r)


"""
## Tweets Prediction ##
"""

############ End page content ############