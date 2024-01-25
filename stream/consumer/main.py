from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

conf = SparkConf().setAppName("stream-preprocessing").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", "thrift://hive-metastore:9083")

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

kafka_data = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:19092") \
  .option("subscribe", "stream") \
  .option("startingOffsets", "earliest") \
  .load()

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price_each", DoubleType(), True),
    StructField("order_date", TimestampType(), True),
    StructField("CustomerShippingAddress", StringType(), True),
    StructField("city_store", StringType(), True),
    StructField("category", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age_range", StringType(), True),
    StructField("discount", DoubleType(), True)
])

json_df = kafka_data.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

processed_df = json_df.select([col(c).alias(c.replace(" ", "").replace("-", "")) for c in json_df.columns])
processed_df = processed_df.drop("CustomerShippingAddress")

def write_to_hive(batch_df, batch_id):
    batch_df.write.saveAsTable("structured", mode="append")

processed = processed_df.writeStream \
    .outputMode("update") \
    .trigger(processingTime='1 minute') \
    .foreachBatch(write_to_hive) \
    .start()

query_raw = kafka_data.writeStream.outputMode("update").trigger(processingTime='1 minute').foreachBatch(lambda batch_df, batch_id: batch_df.write.saveAsTable("raw", mode="append")).start()
query_raw.awaitTermination()

processed.awaitTermination()
