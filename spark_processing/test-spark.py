import findspark
findspark.init('C:/AIML/spark-3.5.4-bin-hadoop3/spark-3.5.4-bin-hadoop3')

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestApp") \
    .master("local[*]") \
    .config("spark.submit.deployMode", "client") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
spark.range(10).show()
