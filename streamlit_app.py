#from collections import namedtuple
#import altair as alt
#import math
#import kafka
from enum import auto
import time
from datetime import datetime
import pandas as pd
from sqlalchemy import column
import streamlit as st
import pyodbc
import pydeck as pdk
from streamlit_autorefresh import st_autorefresh
import pickle
from confluent_kafka import Consumer

# kafka connection
KAFKA_TOPIC   = "twitter_alert"
KAFKA_CONFIG = {'bootstrap.servers': "pkc-w7d6j.germanywestcentral.azure.confluent.cloud:9092",
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': '6YP7QHDEC2T6XW7K',
                'sasl.password': '1/N+18NwcDkHg44bINQqEUNQ3ID0th92aVyrl8Zo28kJx5Z85lhiP22VR/5bMoRN',
                'group.id': '1',
                'auto.offset.reset': 'smallest'}

# map data
MAP_POINT_DATA = pd.DataFrame(
    [
       [-74.0060152,40.741895,'NewYork', 0],
       [16.3725042,48.2083537, 'Vienna', 0],
       [13.3888599,52.5170365, 'Berlin', 0],
       [2.320041, 48.8588897, 'Paris', 0],
       [-0.1276474, 51.5073219, 'London', 0],
       [37.618423, 55.751244, 'Moskow', 0]
    ],
    columns = ['lng', 'lat', 'city', 'tweetsperminute']
)

# Initialize SQL connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_db_connection():
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
db_conn = init_db_connection()

# run only once
# set up Kafka connection
@st.experimental_singleton
def init_kafka_connection():
    kafka_connect = Consumer(KAFKA_CONFIG)
    kafka_connect.subscribe([KAFKA_TOPIC])
    print("Kafka connection established!")
    return kafka_connect
kafka_conn = init_kafka_connection()

# Perform query.
# Uses st.experimental_memo to only rerun when the query changes or after 10 secs.
@st.experimental_memo(ttl=10)
def run_query(query):
    with db_conn.cursor() as cur:
        cur.execute(query)
        return cur.fetchall()

def getTweets(tag, minutes):
    tweets = run_query(f"select * from twitter_api.TweetText where DATEDIFF(second, insertedAt, GETDATE()) < {minutes*60} and Tag like '{tag}' order by insertedAt desc")
    lst = []
    for row in tweets:
        lst.insert(len(lst), [row[2], row[0], row[1]])
    return pd.DataFrame(lst, columns=('Tag', 'Text', 'Time'))

def getTags():
    tags = run_query(f"select distinct TagName from twitter_api.HashtagAggregations order by TagName")
    lst = ['%']
    for row in tags:
        lst.insert(len(lst), row[0])
    return pd.DataFrame(lst)

def getTweetsPerMinute(map):
    map2 = map.copy(deep=False)
    i = 0

    tpm = pd.read_sql_query("WITH TPM AS\
                        (\
                        SELECT\
                            Tagname, Count, Start, [End],\
                            ROW_NUMBER() OVER(PARTITION BY Tagname ORDER BY Start DESC) AS 'RowNumber'\
                            FROM twitter_api.HashtagAggregations\
                        )\
                        SELECT Tagname, max(Count) as Count, Start \
                        FROM TPM \
                        WHERE datediff(second, Start, (select max([Start]) from TPM)) < 120\
                        and datediff(second, Start, (select max([Start]) from TPM)) >= 60\
                        group by Tagname, Start", db_conn)

    df_tpm = pd.DataFrame(tpm, columns =['Tagname', 'Count', 'Start'])
    print(df_tpm)

    # while i<len(map2):
    #     tpm = run_query(f"select sum(count) as tpm from twitter_api.HashtagAggregations where datediff(second, LastUpdatedAt, getdate()) <= 60 and Tagname='{map2.iloc[i]['city']}' group by Tagname")
    #     if len(tpm) > 0:
    #         map2.loc[i, 'tweetsperminute'] = tpm[0][0]
    #     i = i+1
    #print(map2)
    return map2



############ Display page content ############
count = st_autorefresh(interval=15000, key="refreshpage")
# refresh every 15 seconds to get actual data (no other option in streamlit)

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
## Alerting ##
"""
messages = kafka_conn.consume(3, timeout=5)
alert = False
for message in messages:
    if(message.error() == None):
        print(message.timestamp())
        if(message.timestamp()[0] >= datetime.timestamp(datetime.now())-120):
            print("neu!")
            alert = True
        else:
            print("alt!")

if alert==True:
    st.image('images/alert.gif', width=100, output_format='auto')
    'Panic mode activated!'
else:
    st.image('images/check.png', width=100, output_format='auto')
    'No alerts! Everything ok!'


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

# close connections
#conn.close()