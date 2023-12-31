#!/bin/bash/python

# from pyspark import SparkConf
# from pyspark.sql import SparkSession

# conf = SparkConf().setAppName("batch-preprocessing").setMaster("spark://spark-master:7077")

# conf.set("hive.metastore.uris", "thrift://hive-metastore:9083")

# spark = SparkSession.builder.enableHiveSupport().config(conf=conf).getOrCreate()

# spark.sql("USE default")
# df = spark.read.csv("hdfs://namenode:9000/data/batch.csv")
# df.show()

# df.write.mode("overwrite").saveAsTable("batch-data")
# spark.stop()

import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

conf = SparkConf().setAppName("batch-preprocessing").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", "thrift://hive-metastore:9083")

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
text_file = spark.read.csv(
    "hdfs://namenode:9000/data/batch.csv"
)

#text_file.show()

# rdd = text_file.rdd
# result = (
#     rdd.flatMap(lambda line: line.value.split(" "))
#     .filter(lambda word: len(word) > 5)
#     .map(lambda word: (word, 1))
#     .reduceByKey(lambda a, b: a + b)
#     .filter(lambda pair: pair[1] > 50)
# )
# result_df = result.toDF(["Word", "Count"])
text_file.write.mode("overwrite").saveAsTable("test")