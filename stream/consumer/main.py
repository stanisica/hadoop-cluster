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
    StructField("OrderID", IntegerType(), True),
    StructField("Product", StringType(), True),
    StructField("QuantityOrdered", IntegerType(), True),
    StructField("PriceEach", StringType(), True),  
    StructField("OrderDate", StringType(), True),
    StructField("CustomerShippingAddress", StringType(), True),
    StructField("CityStore", StringType(), True),
    StructField("Category", StringType(), True),
    StructField("CustomerGender", StringType(), True),
    StructField("CustomerAgeRange", StringType(), True),
    StructField("Discount", StringType(), True)  
])

json_df = kafka_data.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

processed_df = json_df \
    .withColumn("OrderDate", to_timestamp(col("OrderDate"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("PriceEach", col("PriceEach").cast(DoubleType())) \
    .withColumn("Discount", regexp_replace(col("Discount"), ",", ".").cast(DoubleType()))

def write_to_hive(batch_df, batch_id):
    batch_df.write.saveAsTable("raw", mode="append")

query_raw = processed_df.writeStream \
    .outputMode("update") \
    .trigger(processingTime='1 minute') \
    .foreachBatch(write_to_hive) \
    .start()

query_raw.awaitTermination()
#query_raw = kafka_data.writeStream.outputMode("update").trigger(processingTime='1 minute').foreachBatch(lambda batch_df, batch_id: batch_df.write.saveAsTable("raw", mode="append")).start()

