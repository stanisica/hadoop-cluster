import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def map_category_views(row):
    category_id = row.category_id
    event_type = row.event_type

    if event_type == "view":
        return (category_id, 1)
    else:
        return (category_id, 0)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

# os.environ['PYSPARK_PYTHON'] = sys.executable
# os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

conf = SparkConf().setAppName("batch-preprocessing").setMaster("spark://spark-master:7077")
conf.set("spark.sql.warehouse.dir", "/hive/warehouse")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
csv = spark.read.option("header", "true").csv(HDFS_NAMENODE + "/data/batch.csv")

rdd = csv.rdd
result = (
    rdd.map(map_category_views)
    .filter(lambda x: x[0] is not None)
    .reduceByKey(lambda a, b: a + b)
    .max(key=lambda x: x[1])
)

result_rdd = spark.sparkContext.parallelize([Row(category_id=result[0], views=result[1])])
result_df = spark.createDataFrame(result_rdd)
result_df.write.mode("overwrite").saveAsTable("most_viewed_ctg")