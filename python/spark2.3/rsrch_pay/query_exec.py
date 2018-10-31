from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

conf = SparkConf().setAppName('test_med_analysis')
sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

spark = SparkSession \
    .builder \
    .appName("RPAY_DS_QUERIES") \
    .getOrCreate()

sqlContext.setConf("spark.sql.shuffle.partitions", sc.defaultParallelism)

spark = SparkSession \
    .builder \
    .appName("RPAY_DS_QUERIES") \
    .getOrCreate()

rpay_df = spark.read.parquet('/tmp/spark_poc1/rpay/yr_part')

grouping_cols = ['Teaching_Hospital_Name',
                 'Principal_Investigator_1_Specialty',
                 'Principal_Investigator_2_Specialty',
                 'Principal_Investigator_3_Specialty',
                 'Principal_Investigator_4_Specialty',
                 'Principal_Investigator_5_Specialty']

q1_result = rpay_df.groupBy(grouping_cols) \
                   .agg({'Total_Amount_of_Payment_USDollars': 'sum'}) \
                   .repartition(1).write.mode('overwrite').json('/tmp/spark_poc1/rpay/query_out')
