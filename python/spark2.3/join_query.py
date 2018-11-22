from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

conf = SparkConf().setAppName('GPAY_DS_QUERIES')
sc = SparkContext(conf=conf)
spark = SparkSession \
       .builder \
       .appName("GPAY_DS_QUERIES") \
       .getOrCreate()

sqlContext = SQLContext(sc)

sqlContext.setConf("spark.sql.shuffle.partitions", sc.defaultParallelism)


df1 = spark.read.option("mergeSchema", "true").parquet("/tmp/spark_poc1/gpay/total_data_yr_part")

df2 = spark.read.option("mergeSchema", "true").parquet("/tmp/spark_poc1/rpay/yr_part")

p1 = df1.select("Physician_Specialty", "Program_Year", "Total_Amount_of_Payment_USDollars").groupBy("Physician_Specialty", "Program_Year").agg({"Total_Amount_of_Payment_USDollars": "sum"}).withColumnRenamed("sum(Total_Amount_of_Payment_USDollars)", "Gnrl_funding")

p2 = df2.select("Physician_Specialty", "Program_Year", "Total_Amount_of_Payment_USDollars").groupBy("Physician_Specialty", "Program_Year").agg({"Total_Amount_of_Payment_USDollars": "sum"}).withColumnRenamed("sum(Total_Amount_of_Payment_USDollars)", "Rsrch_funding")

p1.join(p2, ['Physician_Specialty', 'Program_Year'], 'inner').select('Physician_Specialty', 'Program_Year', (col('Gnrl_funding')+col('Rsrch_funding'))).withColumnRenamed('(Gnrl_funding + Rsrch_funding)', 'Total_Funding').coalesce(1).write.mode('overwrite').json('/tmp/spark_poc1/query_out/join_query')
