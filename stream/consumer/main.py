from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from functools import partial

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

# Raw data
processed_df = json_df \
    .withColumn("OrderDate", to_timestamp(col("OrderDate"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("PriceEach", col("PriceEach").cast(DoubleType())) \
    .withColumn("Discount", regexp_replace(col("Discount"), ",", ".").cast(DoubleType()))

# Prosečna Količina Naručena po Proizvodu svakih 5 minuta u prozoru od 10 minuta:
avg_quantity = processed_df \
    .withWatermark("OrderDate", "10 minutes") \
    .groupBy(
        "Product",
        window("OrderDate", "10 minutes", "5 minutes")
    ).agg(round(avg("QuantityOrdered"), 2).alias("avg_quantity"))

# Ukupan Broj Narudžbi po Gradu Magacina svakih 10 minuta u prozoru od 20 minuta:
total_orders_city = processed_df \
    .withWatermark("OrderDate", "20 minutes") \
    .groupBy(
        "CityStore",
        window("OrderDate", "20 minutes", "10 minutes")
    ).agg(count("OrderID").alias("total_orders"))

# Broj Naručenih Proizvoda po Kategoriji svakih 5 minuta u prozoru od 10 min:
count_orders_category = processed_df \
    .withWatermark("OrderDate", "10 minutes") \
    .groupBy(
        "Category",
        window("OrderDate", "10 minutes", "5 minutes")
    ).agg(count("OrderID").alias("order_count"))

def write_to_hive(batch_df, batch_id, table_name):
    batch_df.write.saveAsTable(table_name, mode="append")

write_to_hive_raw = partial(write_to_hive, table_name="raw")
query_raw = processed_df.writeStream \
    .outputMode("update") \
    .trigger(processingTime='1 minute') \
    .foreachBatch(write_to_hive_raw) \
    .start()

write_to_hive_avg_quantity = partial(write_to_hive, table_name="avg_quantity")
query_avg_quantity = avg_quantity.writeStream \
    .outputMode("update") \
    .trigger(processingTime='1 minute') \
    .foreachBatch(write_to_hive_avg_quantity) \
    .start()

write_to_hive_total_orders = partial(write_to_hive, table_name="total_orders_by_city")
query_total_orders_city = total_orders_city.writeStream \
    .outputMode("update") \
    .trigger(processingTime='1 minute') \
    .foreachBatch(write_to_hive_total_orders) \
    .start()

write_to_hive_orders_by_category = partial(write_to_hive, table_name="orders_by_category")
query_count_orders_category = count_orders_category.writeStream \
    .outputMode("update") \
    .trigger(processingTime='1 minute') \
    .foreachBatch(write_to_hive_orders_by_category) \
    .start()

query_raw.awaitTermination()
query_avg_quantity.awaitTermination()
query_total_orders_city.awaitTermination()
query_count_orders_category.awaitTermination()