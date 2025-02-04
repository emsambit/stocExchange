import os
# from pyspark.sql import SparkSession
checkpoint_dir = "C:/AIML/stockExchange/checkpoints/order_matcher"
if os.path.exists(checkpoint_dir):
    import shutil
    shutil.rmtree(checkpoint_dir)

# Set HADOOP_HOME environment variable
os.environ['HADOOP_HOME'] = 'C:/AIML/hadoop/3.2/winutils-master/hadoop-3.2.2'  # Replace with your Hadoop directory
os.environ['PATH'] += os.pathsep + os.path.join(os.environ['HADOOP_HOME'], 'bin')


import findspark
findspark.init('C:/AIML/spark-3.5.4-bin-hadoop3/spark-3.5.4-bin-hadoop3')



from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# spark = SparkSession.builder.appName("OrderMatcher").config("spark.local.dir", "C:/AIML/spark-3.2.1-bin-hadoop3.2/spark-3.2.1-bin-hadoop3.2/").getOrCreate()
spark = SparkSession.builder \
    .appName("OrderMatcher") \
    .master("local[*]") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.hadoop.io.nativeio.disable", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("trader_id", LongType()),
    StructField("instrument", StringType()),
    StructField("type", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", LongType()),
    StructField("timestamp", DoubleType())
])

orders = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "orders_input") \
    .load()

orders_df = orders.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")).select("data.*")

# Matching logic (simplified for demo)
buy_orders = orders_df.filter(col("type") == "buy").alias("buy")
sell_orders = orders_df.filter(col("type") == "sell").alias("sell")

matched_trades = buy_orders.join(sell_orders, ["instrument", "price"]) \
    .withColumn("matched_quantity", expr("LEAST(buy.quantity, sell.quantity)"))

query = matched_trades.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("topic", "matched_trades") \
    .option("checkpointLocation", "C:/AIML/stockExchange/checkpoints/order_matcher") \
    .start()


# query = matched_trades.selectExpr("to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("console") \
#     .outputMode("append") \
#     .option("truncate", "false") \
#     .start()

query.awaitTermination()
