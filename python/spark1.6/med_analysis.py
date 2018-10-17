from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import time

conf = SparkConf().setAppName('test_med_analysis')
sc = SparkContext(conf=conf)
sqlc = SQLContext(sc)

gpay_df = sqlc.read.parquet('hdfs://jana-hadoop-mgr-1:8020/tmp/parq_out')
st = time.time()
q1_out = gpay_df.groupBy(gpay_df.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, gpay_df.Physician_Specialty).agg({'Total_Amount_of_Payment_USDollars': 'sum'}).collect()
et = time.time()
print "time taken with default partition: ", et - st
