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

paths = ['/tmp/spark_poc/repart_out']
df = spark.read.parquet(*paths)

#df.repartition(sc.defaultParallelism).write.parquet('/tmp/spark_poc/repart_out')
#print df.rdd.getNumPartitions()
#df.write.partitionBy('Program_Year').parquet('/tmp/spark_poc/part_yr_out')

df_13 = df.filter('Program_Year == 2013').groupBy('Physician_Specialty').agg({'Total_Amount_of_Payment_USDollars': 'sum'}).withColumnRenamed('sum(Total_Amount_of_Payment_USDollars)', 'Total_Amount_2013')

df_14 = df.filter('Program_Year == 2014').groupBy('Physician_Specialty').agg({'Total_Amount_of_Payment_USDollars': 'sum'}).withColumnRenamed('sum(Total_Amount_of_Payment_USDollars)', 'Total_Amount_2014')

df_15 = df.filter('Program_Year == 2015').groupBy('Physician_Specialty').agg({'Total_Amount_of_Payment_USDollars': 'sum'}).withColumnRenamed('sum(Total_Amount_of_Payment_USDollars)', 'Total_Amount_2015')

df_16 = df.filter('Program_Year == 2016').groupBy('Physician_Specialty').agg({'Total_Amount_of_Payment_USDollars': 'sum'}).withColumnRenamed('sum(Total_Amount_of_Payment_USDollars)', 'Total_Amount_2016')

df_17 = df.filter('Program_Year == 2017').groupBy('Physician_Specialty').agg({'Total_Amount_of_Payment_USDollars': 'sum'}).withColumnRenamed('sum(Total_Amount_of_Payment_USDollars)', 'Total_Amount_2017')

total_amt = df.groupBy('Physician_Specialty').agg({'Total_Amount_of_Payment_USDollars': 'sum'}).withColumnRenamed('sum(Total_Amount_of_Payment_USDollars)', 'Total_Amount_All_years')

result_df = total_amt.join(df_13, 'Physician_Specialty', 'outer')
result_df = result_df.join(df_14, 'Physician_Specialty', 'outer')
result_df = result_df.join(df_15, 'Physician_Specialty', 'outer')
result_df = result_df.join(df_16, 'Physician_Specialty', 'outer')
result_df = result_df.join(df_17, 'Physician_Specialty', 'outer')


result_df.repartition(1).write.mode('overwrite').json('/tmp/spark_poc/rpay/all_query_out')
