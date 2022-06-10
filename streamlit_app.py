from datetime import datetime
from operator import index
import pandas as pd
import sqlalchemy 
from sqlalchemy import column
import streamlit as st
#import pytz
import pydeck as pdk
from streamlit_autorefresh import st_autorefresh
import pickle
from confluent_kafka import Consumer
import matplotlib.pyplot as plt
import plotly.express as px
import numpy as np
import altair as alt

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
    return sqlalchemy.create_engine(
        "mssql+pyodbc://"
        + st.secrets["username"] + ":" + st.secrets["password"]
        + "@" + st.secrets["server"]
        + "/" + st.secrets["database"]        
        + "?driver=ODBC+Driver+17+for+SQL+Server"
    )
db_conn = init_db_connection()

# run only once
# set up Kafka connection
# @st.experimental_singleton
# def init_kafka_connection():
#     kafka_connect = Consumer(KAFKA_CONFIG)
#     kafka_connect.subscribe([KAFKA_TOPIC])
#     print("Kafka connection established!")
#     return kafka_connect
# kafka_conn = init_kafka_connection()

# run only once
# import pickle model autoregression
@st.experimental_singleton
def load_model():
    # loading the trained model
    pickle_in = open('data/autoregression.pkl', 'rb') 
    regressor = pickle.load(pickle_in)
    return regressor
regr_model = load_model()

# get tweets of the last x minutes for a specific tag
def getTweets(tag, minutes):
    try:
        tweets = pd.read_sql_query(f"select Tag, Content, insertedAt from twitter_api.TweetText \
                                    where DATEDIFF(second, insertedAt, GETDATE()) < {(minutes+2)*60} and Tag like '{tag}' \
                                    order by insertedAt desc", db_conn)
    except:
        tweets = pd.DataFrame()
    return tweets

def getTags():
    try:
        tags = pd.read_sql_query("select '%' as TagName union select distinct TagName \
                                from twitter_api.HashtagAggregations order by TagName", db_conn)
    except:
        tags = pd.DataFrame()
    return tags

def getTweetsPerMinute(map):
    map2 = map.copy(deep=False)
    i = 0

    try:
        tpm = pd.read_sql_query("WITH TPM AS\
                            (\
                            SELECT\
                                Tagname, Count, Start, [End],\
                                ROW_NUMBER() OVER(PARTITION BY Tagname ORDER BY Start DESC) AS 'RowNumber'\
                                FROM twitter_api.HashtagAggregations\
                            )\
                            SELECT Tagname, max(Count) as Count, Start \
                            FROM TPM \
                            WHERE datediff(second, Start, (select max([Start]) from TPM)) < 180\
                            and datediff(second, Start, (select max([Start]) from TPM)) >= 120\
                            group by Tagname, Start", db_conn)

        df_tpm = pd.DataFrame(tpm, columns =['Tagname', 'Count', 'Start'])

        while i<len(map2):
            if len(df_tpm[df_tpm['Tagname'] == map2.loc[i, 'city']]) > 0:
                rw = df_tpm.loc[df_tpm['Tagname'] == map2.loc[i, 'city']]
                map2.loc[i, 'tweetsperminute'] = rw.iloc[0]['Count']
            else:
                map2.loc[i, 'tweetsperminute'] = 0
            i = i+1
    except:
        map2 = map2
    return map2

def getTweetHistoryForParis():
    # get last 15 entries
    history = pd.read_sql_query("WITH TPM AS \
                                    ( \
                                        SELECT Tagname, \
                                                Count, \
                                                Start, \
                                                ROW_NUMBER() OVER (PARTITION BY Tagname ORDER BY Start DESC) AS 'RowNumber' \
                                        FROM twitter_api.HashtagAggregations \
                                    ) \
                                SELECT top 15 Tagname, max(Count) as Count, Start \
                                FROM TPM \
                                WHERE datediff(second, Start, (select max([Start]) from TPM)) >= 120 \
                                and Tagname = 'Paris' \
                                group by Tagname, Start \
                                order by Tagname, Start desc;", db_conn)
    history = history.iloc[::-1] # reverse
    return history

def getRetweets():
    try:
        rtw = pd.read_sql_query("select Top 5 REPLACE(Content, 'RT ', ''), count(Content) as count from twitter_api.TweetText \
                                where insertedAt > convert(date, getdate()) \
                                group by Content \
                                order by count desc", db_conn)
    except:
        rtw = pd.DataFrame()
    return rtw



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



# """
# ## Alerting ##
# """
# messages = kafka_conn.consume(3, timeout=5)
# alert = False
# print(messages)
# for message in messages:
#     if(message.error() == None):
#         print(message.timestamp())
#         if(message.timestamp()[0] >= datetime.timestamp(datetime.now(pytz.timezone("GMT")))-120):
#             print("neu!")
#             alert = True
#         else:
#             print("alt!")

# if alert==True:
#     st.image('images/alert.gif', width=100, output_format='auto')
#     'Panic mode activated!'
# else:
#     st.image('images/check.png', width=100, output_format='auto')
#     'No alerts! Everything ok!'


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
## Tweets Prediction for #Paris ##
"""
forecast = 5
window = 15
coef = regr_model.params
history = getTweetHistoryForParis()
history1 = history['Count'].values.tolist()

# predict
history = [history1[i] for i in range(len(history1))]
predictions = list()
for t in range(forecast):
    length = len(history)
    lag = [history[i] for i in range(length-window,length)]
    yhat = coef[0]
    for d in range(window):
        yhat += coef[d+1] * lag[window-d-1]
    predictions.append(yhat)
    history.append(yhat)

# plot 
df_hist = pd.DataFrame(dict(art="History", value=history1, time=list(range(-14,1,1))))
df_pred = pd.DataFrame(dict(art="Prediction", value=predictions, time=list(range(1,6,1))))
df_source = pd.concat([df_hist, df_pred])

chart = alt.Chart(df_source).mark_line().encode(
    x='time',
    y='value',
    color='art',
    strokeDash='art',
)

st.altair_chart(chart)

"""
## Today's most Retweeted
"""
df_retweets = getRetweets()
rttable = st.empty()
rttable.dataframe(df_retweets)

############ End page content ############
