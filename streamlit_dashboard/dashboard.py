import streamlit as st
from kafka import KafkaConsumer
import json
import pandas as pd

# Set up Kafka consumer
consumer = KafkaConsumer(
    'analytics_output',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Streamlit App Layout
st.title("Real-Time Stock Analytics Dashboard")
st.markdown("---")

st.header("Simple Moving Averages (SMA)")


# Function to fetch and display data
def fetch_data():
    data_list = []
    for message in consumer:
        data = message.value
        window_start = data['window']['start']
        window_end = data['window']['end']
        instrument = data['instrument']
        sma = data['sma']

        data_list.append({
            'Window Start': window_start,
            'Window End': window_end,
            'Instrument': instrument,
            'SMA': sma
        })

        if len(data_list) > 20:  # Display the last 20 entries
            data_list.pop(0)

        df = pd.DataFrame(data_list)
        st.write(df)


# Display data
if st.button('Start Streaming'):
    fetch_data()
