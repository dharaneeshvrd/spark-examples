from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("RPAY_DS_QUERIES") \
    .getOrCreate()

rpay_df = spark.read.option('mergeSchema', 'true').parquet('/tmp/spark_poc/parq_out_rsrch_2013',
                                                            '/tmp/spark_poc/parq_out_rsrch_2014',
                                                            '/tmp/spark_poc/parq_out_rsrch_2015',
                                                            '/tmp/spark_poc/parq_out_rsrch_2016',
                                                            '/tmp/spark_poc/parq_out_rsrch_2017')

grouping_cols = ['Teaching_Hospital_Name',
                 'Principal_Investigator_1_Specialty',
                 'Principal_Investigator_2_Specialty',
                 'Principal_Investigator_3_Specialty',
                 'Principal_Investigator_4_Specialty',
                 'Principal_Investigator_5_Specialty']

q1_result = rpay_df.groupBy(grouping_cols) \
                   .agg({'Total_Amount_of_Payment_USDollars': 'sum'}) \
                   .collect()
print "Resultant row count on Query: ", len(q1_result)
