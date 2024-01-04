import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def map_category_views(row):
    category_id = row.category_id
    event_type = row.event_type
    category_code = row.category_code if row.category_code is not None else "noname"

    if event_type == "view":
        return ((category_id,category_code), 1)
    else:
        return ((category_id,category_code), 0)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("batch-preprocessing").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
csv = spark.read.option("header", "true").csv(HDFS_NAMENODE + "/data/batch.csv")

rdd = csv.rdd
result = (
    rdd.map(map_category_views)
    .reduceByKey(lambda a, b: a + b)
    .map(lambda x: Row(category_id=x[0][0], category_code=x[0][1], views=x[1]))
)

result_df = spark.createDataFrame(result)
result_df.write.mode("overwrite").saveAsTable("ctg_views")
