import os

# from pyspark.sql import SparkSession
checkpoint_dir = "C:/AIML/stockExchange/checkpoints/sma_calculator"

if os.path.exists(checkpoint_dir):
    import shutil
    shutil.rmtree(checkpoint_dir)

# Set HADOOP_HOME environment variable
os.environ['HADOOP_HOME'] = 'C:/AIML/hadoop/3.2/winutils-master/hadoop-3.2.2'  # Replace with your Hadoop directory
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')


import findspark
findspark.init('C:/AIML/spark-3.5.4-bin-hadoop3/spark-3.5.4-bin-hadoop3')

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

spark = SparkSession.builder \
    .appName("OrderMatcher") \
    .master("local[*]") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.io.nativeio.disable", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

trades = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "matched_trades") \
    .load()

# Define the schema to parse JSON data from Kafka
schema = StructType([
    StructField("trader_id", LongType()),
    StructField("instrument", StringType()),
    StructField("type", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", LongType()),
    StructField("timestamp", DoubleType())  # Make sure timestamp is in epoch format
])

# Read matched trades from Kafka
trades = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "matched_trades") \
    .load()

# Parse the JSON messages and extract required fields
trades_df = trades.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.instrument", "data.price", "data.timestamp")

# Convert DOUBLE timestamp (epoch seconds) to TIMESTAMP type
trades_df = trades_df.withColumn("timestamp", (col("timestamp") / 1000).cast("timestamp"))

# Add watermark to handle late data
trades_with_watermark = trades_df.withWatermark("timestamp", "10 minutes")

# Calculate Simple Moving Average (SMA)
sma_df = trades_df.withWatermark("timestamp", "10 minutes").groupBy(
    window(col("timestamp").cast("timestamp"), "10 minutes", "5 minutes"),  # Convert timestamp to proper type
    col("instrument")
).agg(avg("price").alias("sma"))

# query = sma_df.selectExpr("to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("console") \
#     .option("truncate", "false") \
#     .outputMode("update") \
#     .start()


# Write the SMA output back to Kafka
query = sma_df.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("topic", "analytics_output") \
    .option("checkpointLocation", "C:/AIML/stockExchange/checkpoints/sma_calculator") \
    .outputMode("update") \
    .start()

query.awaitTermination()