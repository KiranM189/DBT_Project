import streamlit as st

from kafka import KafkaConsumer

import json

import pandas as pd

from datetime import datetime

import altair as alt



# ----------- Page Setup -----------

st.set_page_config(layout="wide", page_title="NYC Taxi Dashboard")



# Custom CSS for better styling

st.markdown("""

    <style>

    .metric-card {

        background-color: #0E1117;

        border-radius: 10px;

        padding: 15px;

        margin-bottom: 15px;

        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);

        border-left: 4px solid #4CAF50;

    }

    .metric-title {

        font-size: 14px;

        color: #9EA6B0;

        margin-bottom: 5px;

        font-weight: 500;

    }

    .metric-value {

        font-size: 28px;

        font-weight: bold;

        color: #FFFFFF;

        margin-top: 5px;

    }

    .metric-unit {

        font-size: 12px;

        color: #9EA6B0;

    }

    .refresh-button {

        margin-bottom: 20px;

        background-color: #4CAF50;

        color: white;

        border: none;

        border-radius: 4px;

        padding: 8px 16px;

        font-weight: 500;

    }

    .section-header {

        border-bottom: 2px solid #4CAF50;

        padding-bottom: 8px;

        margin-bottom: 20px;

        color: #FFFFFF;

    }

    .chart-container {

        background-color: #0E1117;

        border-radius: 10px;

        padding: 15px;

        margin-bottom: 15px;

    }

    .warning-box {

        background-color: #FFF3CD;

        color: #856404;

        padding: 12px;

        border-radius: 4px;

        margin-bottom: 20px;

        border-left: 4px solid #FFC107;

    }

    .info-box {

        background-color: #E7F5FF;

        color: #0C5460;

        padding: 12px;

        border-radius: 4px;

        margin-bottom: 20px;

        border-left: 4px solid #17A2B8;

    }

    .last-updated {

        text-align: right;

        color: #6C757D;

        font-size: 12px;

        margin-top: 20px;

        font-style: italic;

    }

    </style>

    """, unsafe_allow_html=True)



st.title("🚕 NYC Taxi Dashboard")



# Kafka Consumer Setup

def get_consumer(topic, group):

    return KafkaConsumer(

        topic,

        bootstrap_servers='localhost:9092',

        auto_offset_reset='earliest',

        enable_auto_commit=True,

        group_id=group,

        value_deserializer=lambda m: json.loads(m.decode('utf-8')),

        consumer_timeout_ms=1000

    )



# Session state initialization

if "fare_summary" not in st.session_state:

    st.session_state.fare_summary = {}

if "speed_fare" not in st.session_state:

    st.session_state.speed_fare = {}



# Data fetching functions

def update_fare_summary():

    consumer = None

    try:

        consumer = get_consumer('taxi_fare_summary', 'fare-dashboard')

        records = consumer.poll(timeout_ms=1000)

        for tp, batch in records.items():

            for msg in batch:

                data = msg.value

                key = data['trip_date']

                st.session_state.fare_summary[key] = data

    except Exception as e:

        st.error(f"Error fetching fare data: {str(e)}")

    finally:

        if consumer:

            consumer.close()



def update_speed_fare():

    consumer = None

    try:

        consumer = get_consumer('taxi_speed_fare', 'speedfare-dashboard')

        records = consumer.poll(timeout_ms=1000)

        for tp, batch in records.items():

            for msg in batch:

                data = msg.value

                timestamp = f"{data['start_time']}|{data['end_time']}"

                st.session_state.speed_fare[timestamp] = data

    except Exception as e:

        st.error(f"Error fetching speed data: {str(e)}")

    finally:

        if consumer:

            consumer.close()



# Refresh button

if st.button("🔄 Refresh Dashboard", key="refresh_button"):

    st.rerun()



# Update data

update_fare_summary()

update_speed_fare()



# Display fare data

if st.session_state.fare_summary:

    fare_df = pd.DataFrame(st.session_state.fare_summary.values())

    fare_df["trip_date"] = pd.to_datetime(fare_df["trip_date"])

    fare_df = fare_df.sort_values("trip_date")



    st.markdown("<h2 class='section-header'>📊 Daily Fare Metrics</h2>", unsafe_allow_html=True)

    

    cols = st.columns(5)

    metrics = [

        ("💰 Tips", "total_tip", "{:,.2f}", ""),

        ("✈️ Airport Fees", "total_airport", "{:,.2f}", ""),

        ("🛣️ Tolls", "total_tolls", "{:,.2f}", ""),

        ("🚕 Fare", "total_fare", "{:,.2f}", ""),

        ("📊 Total Collected", "total_total", "{:,.2f}", "")

    ]



    for i, (title, col_name, fmt, unit) in enumerate(metrics):

        with cols[i]:

            value = fare_df[col_name].sum()

            st.markdown(f"""

            <div class="metric-card">

                <div class="metric-title">{title}</div>

                <div class="metric-value">{fmt.format(value)}</div>

                <div class="metric-unit">{unit}</div>

            </div>

            """, unsafe_allow_html=True)



    st.markdown("<div class='chart-container'>", unsafe_allow_html=True)

    chart = alt.Chart(fare_df).mark_line(

        point=True,

        color='#4CAF50',

        strokeWidth=2

    ).encode(

        x=alt.X('trip_date:T', title='Date', axis=alt.Axis(format='%b %Y')),

        y=alt.Y('total_total:Q', title='Total Collected ($)'),

        tooltip=['trip_date:T', 'total_total:Q']

    ).properties(

        height=350,

        title='Total Collected Over Time'

    ).configure_axis(

        gridColor='#2A2E35',

        domainColor='#6C757D',

        labelColor='#9EA6B0',

        titleColor='#FFFFFF'

    )

    st.altair_chart(chart, use_container_width=True)

    st.markdown("</div>", unsafe_allow_html=True)



    with st.expander("📋 View Raw Fare Data", expanded=False):

        st.dataframe(fare_df, use_container_width=True)

else:

    st.markdown("""

    <div class="warning-box">

        ⚠️ No fare data available yet. Waiting for data stream...

    </div>

    """, unsafe_allow_html=True)



# Display speed/fare data

if st.session_state.speed_fare:

    speed_df = pd.DataFrame(list(st.session_state.speed_fare.values()))

    speed_df["start_time"] = pd.to_datetime(speed_df["start_time"])

    speed_df["end_time"] = pd.to_datetime(speed_df["end_time"])

    speed_df = speed_df.sort_values("start_time")



    # Create hourly buckets

    speed_df["hour_bucket"] = speed_df["start_time"].dt.floor("H")

    hourly_agg = speed_df.groupby("hour_bucket").agg({

        "avg_fare": "mean",

        "avg_speed": "mean"

    }).reset_index()



    st.markdown("<h2 class='section-header'>⏱️ Hourly Speed & Fare Metrics</h2>", unsafe_allow_html=True)

    

    cols = st.columns(2)

    with cols[0]:

        st.markdown(f"""

        <div class="metric-card">

            <div class="metric-title">💵 Latest Avg Fare</div>

            <div class="metric-value">{speed_df.iloc[-1]['avg_fare']:.2f}</div>

            <div class="metric-unit">USD</div>

        </div>

        """, unsafe_allow_html=True)

    

    with cols[1]:

        st.markdown(f"""

        <div class="metric-card">

            <div class="metric-title">🚀 Latest Avg Speed</div>

            <div class="metric-value">{speed_df.iloc[-1]['avg_speed']:.2f}</div>

            <div class="metric-unit">mph</div>

        </div>

        """, unsafe_allow_html=True)



    # Create tabs for the charts

    tab1, tab2 = st.tabs(["Average Fare", "Average Speed"])

    

    with tab1:

        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)

        fare_chart = alt.Chart(hourly_agg).mark_area(

            line={'color':'#4CAF50', 'size':2},

            color=alt.Gradient(

                gradient='linear',

                stops=[alt.GradientStop(color='#4CAF50', offset=0),

                       alt.GradientStop(color='#0E1117', offset=1)],

                x1=1,

                x2=1,

                y1=1,

                y2=0

            )

        ).encode(

            x=alt.X('hours(hour_bucket):O', title='Hour of Day', axis=alt.Axis(labelAngle=0)),

            y=alt.Y('avg_fare:Q', title='Average Fare ($)', scale=alt.Scale(zero=False)),

            tooltip=['hour_bucket:T', 'avg_fare:Q']

        ).properties(

            height=350,

            title='Hourly Average Fare'

        ).configure_axis(

            gridColor='#2A2E35',

            domainColor='#6C757D',

            labelColor='#9EA6B0',

            titleColor='#FFFFFF'

        )

        st.altair_chart(fare_chart, use_container_width=True)

        st.markdown("</div>", unsafe_allow_html=True)

    

    with tab2:

        st.markdown("<div class='chart-container'>", unsafe_allow_html=True)

        speed_chart = alt.Chart(hourly_agg).mark_area(

            line={'color':'#2196F3', 'size':2},

            color=alt.Gradient(

                gradient='linear',

                stops=[alt.GradientStop(color='#2196F3', offset=0),

                       alt.GradientStop(color='#0E1117', offset=1)],

                x1=1,

                x2=1,

                y1=1,

                y2=0

            )

        ).encode(

            x=alt.X('hours(hour_bucket):O', title='Hour of Day', axis=alt.Axis(labelAngle=0)),

            y=alt.Y('avg_speed:Q', title='Average Speed (mph)', scale=alt.Scale(zero=False)),

            tooltip=['hour_bucket:T', 'avg_speed:Q']

        ).properties(

            height=350,

            title='Hourly Average Speed'

        ).configure_axis(

            gridColor='#2A2E35',

            domainColor='#6C757D',

            labelColor='#9EA6B0',

            titleColor='#FFFFFF'

        )

        st.altair_chart(speed_chart, use_container_width=True)

        st.markdown("</div>", unsafe_allow_html=True)



    with st.expander("📋 View Raw Speed/Fare Data", expanded=False):

        st.dataframe(speed_df.sort_values("start_time"), use_container_width=True)

else:

    st.markdown("""

    <div class="info-box">

        ℹ️ Waiting for speed/fare data to stream in...

    </div>

    """, unsafe_allow_html=True)



st.markdown(f"""

<div class="last-updated">

    Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

</div>

""", unsafe_allow_html=True)