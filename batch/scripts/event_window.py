import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, year, month, dayofmonth, col
from pyspark.sql.types import StructType, StructField, DateType, StringType, DoubleType, IntegerType

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("batch-preprocessing").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
csv = spark.read.option("header", "true").csv(HDFS_NAMENODE + "/batch.csv")

transformed = (
    csv.withColumn("event_date", to_date("event_time"))
       .withColumn("year", year("event_date"))
       .withColumn("month", month("event_date"))
       .withColumn("day", dayofmonth("event_date"))
       .withColumn("price", col("price").cast(DoubleType()))
       .filter(csv.price.isNotNull() & (csv.price > 0))
       .select("event_date", "year", "month", "day", "category_id", "product_id", "price")
)

rdd = transformed.rdd
schema = StructType([
    StructField("event_date", DateType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day", IntegerType(), True),
    StructField("category_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", DoubleType(), True)
])

result_df = spark.createDataFrame(rdd, schema)
result_df.write.mode("overwrite").saveAsTable("event_window")