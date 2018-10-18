from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

import csv
import datetime
import time

conf = SparkConf().setAppName('test_med_analysis')
sc = SparkContext(conf=conf)
spark = SparkSession \
    .builder \
    .appName("med_analysis") \
    .getOrCreate()

rpay_df = spark.read.parquet('hdfs://jana-hadoop-mgr-1:8020/tmp/spark_poc/parq_out_rsrch')
st = time.time()
q1_out = rpay_df.groupBy(['Teaching_Hospital_Name','Principal_Investigator_1_Specialty','Principal_Investigator_2_Specialty','Principal_Investigator_3_Specialty','Principal_Investigator_4_Specialty','Principal_Investigator_5_Specialty']).agg({'Total_Amount_of_Payment_USDollars': 'sum'}).collect()
et = time.time()
print "time taken with default partition: ", et - st
print sc.defaultParallelism
print rpay_df.rdd.getNumPartitions()
