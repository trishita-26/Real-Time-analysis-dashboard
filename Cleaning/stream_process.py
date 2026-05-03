from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("user_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("price", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("event_type", StringType()),
    StructField("timestamp", StringType()),
    StructField("payment_method", StringType()),
    StructField("city", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_topic") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json_str")

parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

clean_df = parsed_df.dropna()

final_df = clean_df.withColumn("revenue", col("price") * col("quantity"))

query = final_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", "C:/spark_checkpoints/ecommerce") \
    .start()

query.awaitTermination()