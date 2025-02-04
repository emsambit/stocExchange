# 📈 Stock Exchange Streaming App

This project simulates a real-time stock exchange environment leveraging **Apache Kafka** for message brokering, **Apache Spark** for real-time data processing, and **Streamlit** for dynamic, real-time data visualization.

## 🚀 Key Features
- **Order Matching:** Efficiently matches buy and sell orders in real-time using Spark Structured Streaming.
- **Real-Time Analytics:** Calculates **Simple Moving Averages (SMA)** for stock instruments with real-time updates.
- **Interactive Dashboard:** Visualize trade and analytics data dynamically with a responsive Streamlit dashboard.

## 🛠 Tech Stack
- **Apache Kafka:** For real-time message streaming between components.
- **Apache Spark:** For processing and aggregating streaming data.
- **Streamlit:** For building the interactive web dashboard.
- **Docker:** For managing Kafka and Zookeeper instances.

## 📂 Project Structure
```
stock_exchange_streaming_app/
|
|-- kafka_setup/
|   |-- docker-compose.yml  # Docker setup for Kafka and Zookeeper
|   |-- create_topics.sh    # Script to create Kafka topics
|
|-- spark_processing/
|   |-- order_matcher.py    # Spark job for matching orders
|   |-- sma_calculator.py   # Spark job for calculating Simple Moving Averages
|
|-- checkpoints/
|   |-- order_matcher/      # Checkpoint directory for order matcher
|   |-- sma_calculator/     # Checkpoint directory for SMA calculator
|
|-- streamlit_app/
|   |-- dashboard.py        # Streamlit app for real-time data visualization
|
|-- requirements.txt        # Python dependencies
|-- README.md               # Project documentation
```

## ⚙️ Setup Instructions

### 1. Start Kafka and Zookeeper:
Navigate to the `kafka_setup` directory and run the following commands:
```bash
cd kafka_setup
docker-compose up -d
./create_topics.sh
```

### 2. Run Spark Streaming Jobs:
In separate terminals, run the following commands to start Spark jobs:
```bash
python spark_processing/order_matcher.py
python spark_processing/sma_calculator.py
```

### 3. Run the Streamlit Dashboard:
Navigate to the `streamlit_app` directory and launch the Streamlit app:
```bash
streamlit run streamlit_app/dashboard.py
```

## 💻 Requirements
Ensure you have the following installed:
- Python 3.x
- Docker
- Apache Spark
- Kafka

Install the required Python dependencies:
```bash
pip install -r requirements.txt
```

## 📊 Testing the Application
1. **Verify Kafka Topics:** Ensure the Kafka topics `orders_input`, `matched_trades`, and `analytics_output` are created.
2. **Check Spark Jobs:** Ensure both `order_matcher.py` and `sma_calculator.py` run without errors.
3. **Streamlit Dashboard:** Use the dashboard to monitor real-time trades and SMA calculations. Ensure the data updates dynamically as expected.

---

Feel free to contribute or raise issues for improvements! 🚀
