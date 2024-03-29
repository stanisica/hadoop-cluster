import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import DoubleType

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("batch-preprocessing").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
csv = spark.read.option("header", "true").csv(HDFS_NAMENODE + "/batch.csv")

transformed = (
    csv.select("event_time", "product_id", "category_id", "category_code", "price")
       .withColumn("event_time", to_date("event_time"))  
       .withColumn("price", col("price").cast(DoubleType()))  
       .filter(col("price").isNotNull() & (col("price") > 0))
)

transformed.write.mode("overwrite").saveAsTable("price_variation")
spark.stop()