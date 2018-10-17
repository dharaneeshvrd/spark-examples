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

gpay_df = spark.read.orc('hdfs://jana-hadoop-mgr-1:8020/tmp/orc_out1_mon_par')
st = time.time()
q1_out = gpay_df.groupBy(gpay_df.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, gpay_df.Physician_Specialty).agg({'Total_Amount_of_Payment_USDollars': 'sum'}).collect()
et = time.time()
print "time taken with default partition: ", et - st
print sc.defaultParallelism
