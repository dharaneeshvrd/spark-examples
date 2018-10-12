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

'''
fetch_month = udf(lambda s: s.month, IntegerType())
gpay_df_with_mon = gpay_df.withColumn('Month_for_Partition', fetch_month(gpay_df['Date_of_Payment']))

gpay_df_mon_with_par = gpay_df_with_mon.repartition(12, 'Month_for_Partition')

st = time.time()
q2_out = gpay_df_mon_with_par.groupBy(gpay_df_mon_with_par.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, gpay_df_mon_with_par.Physician_Specialty).agg({'Total_Amount_of_Payment_USDollars': 'sum'}).collect()
et = time.time()
print "time taken with month partition: ", et - st
'''
