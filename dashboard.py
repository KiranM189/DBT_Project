import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd
from datetime import datetime
import altair as alt

# ----------- Page Setup -----------
st.set_page_config(layout="wide", page_title="Yellow Taxi Dashboard")

st.markdown(
    """
    <style>
    .big-number {
        font-size: 36px;
        color: gold;
        background-color: black;
        padding: 10px 20px;
        border-radius: 10px;
        display: inline-block;
        text-align: center;
    }
    .refresh-button button {
        background-color: gold;
        color: black;
        font-weight: bold;
    }
    </style>
    """, unsafe_allow_html=True
)

st.title("🚖 Yellow Taxi Dashboard")

# ----------- Kafka Consumer Setup -----------
@st.cache_resource
def load_kafka_consumer():
    return KafkaConsumer(
        'taxi_fare_summary',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='streamlit-dashboard',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

# ----------- Refresh Button -----------
if st.button("🔄 Refresh Dashboard"):
    st.rerun()


consumer = load_kafka_consumer()

# ----------- Live Data Collection -----------
# ----------- Preserve Last Data in Session -----------
# ----------- Preserve Last Data in Session -----------
if "latest_data" not in st.session_state:
    st.session_state.latest_data = {}

new_data = {}
poll_records = consumer.poll(timeout_ms=1000)

for tp, messages in poll_records.items():
    for message in messages:
        record = message.value
        trip_date = record.get("trip_date")
        if trip_date:
            new_data[trip_date] = record

if new_data:
    st.session_state.latest_data.update(new_data)
    print("✅ New Kafka messages added.")
else:
    print("⚠️ No new Kafka data received. Using cached data.")

if st.session_state.latest_data:
    df = pd.DataFrame(list(st.session_state.latest_data.values()))
    df["trip_date"] = pd.to_datetime(df["trip_date"])
else:
    st.warning("⚠️ No data available yet. Try refreshing after a few seconds.")
    st.stop()




# ----------- Cards for Aggregated Metrics -----------
total_tip = df["total_tip"].sum()
total_airport = df["total_airport"].sum()
total_tolls = df["total_tolls"].sum()
total_fare = df["total_fare"].sum()
total_extra = df["total_extra"].sum()
total_total = df["total_total"].sum()

# ----------- Cards for Aggregated Metrics (Better Spacing) -----------

st.markdown("### 💡 Daily Metrics")

with st.container():
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(f'<div class="big-number">💰 {total_tip:,.2f}<br><small>Total Tip Amount</small></div>', unsafe_allow_html=True)
    with col2:
        st.markdown(f'<div class="big-number">✈️ {total_airport:,.2f}<br><small>Total Airport Fee</small></div>', unsafe_allow_html=True)
    with col3:
        st.markdown(f'<div class="big-number">🛣️ {total_tolls:,.2f}<br><small>Total Tolls</small></div>', unsafe_allow_html=True)

    st.markdown("")

with st.container():
    col4, col5, col6 = st.columns(3)
    with col4:
        st.markdown(f'<div class="big-number">🚕 {total_fare:,.2f}<br><small>Total Fare</small></div>', unsafe_allow_html=True)
    with col5:
        st.markdown(f'<div class="big-number">➕ {total_extra:,.2f}<br><small>Total Extra Charges</small></div>', unsafe_allow_html=True)
    with col6:
        st.markdown(f'<div class="big-number">📊 {total_total:,.2f}<br><small>Total Collected</small></div>', unsafe_allow_html=True)

st.markdown("---")

# ----------- Time Series Chart -----------
st.subheader("📆 Total Collected Over Time")
line = alt.Chart(df).mark_line(point=True).encode(
    x=alt.X('trip_date:T', title='Date'),
    y=alt.Y('total_total:Q', title='Total Amount ($)'),
    tooltip=['trip_date:T', 'total_total:Q']
).properties(width=900, height=400)
st.altair_chart(line, use_container_width=True)

# ----------- Data Table -----------
with st.expander("📋 View Aggregated Data"):
    st.dataframe(df.sort_values("trip_date", ascending=False), use_container_width=True)
