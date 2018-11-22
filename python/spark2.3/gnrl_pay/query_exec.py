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

# Loading the compressed dataset into a dataframe
gpay_df = spark.read.option('mergeSchema', 'true').parquet('/tmp/spark_poc1/gpay/default_part_schema1', '/tmp/spark_poc1/gpay/default_part_schema2')

# 1st query execution and collecting the result
gpay_df.select(gpay_df.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, gpay_df.Physician_Specialty, gpay_df.Total_Amount_of_Payment_USDollars).groupBy(gpay_df.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, gpay_df.Physician_Specialty).agg({'Total_Amount_of_Payment_USDollars': 'sum'}).write.mode('overwrite').json('/tmp/spark_poc1/query_out/gpay/q1_out')


# 2nd query execution
df1 = gpay_df.select('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name', 'Teaching_Hospital_ID',  'Date_of_Payment', 'Total_Amount_of_Payment_USDollars') \
             .groupBy('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name', 'Teaching_Hospital_ID',  month('Date_of_Payment')) \
             .agg({'Total_Amount_of_Payment_USDollars': 'sum'}) \
             .withColumnRenamed('month(Date_of_Payment)', 'month') \
             .withColumnRenamed('sum(Total_Amount_of_Payment_USDollars)', 'Total_funding_per_month')

df2 = gpay_df.select('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name', 'Teaching_Hospital_ID', 'Total_Amount_of_Payment_USDollars') \
             .groupBy('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name', 'Teaching_Hospital_ID') \
             .agg({'Total_Amount_of_Payment_USDollars': 'sum'}) \
             .withColumnRenamed('sum(Total_Amount_of_Payment_USDollars)', 'Total_funding_per_year')

# collecting the result
q2_result = df1.join(df2, ['Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name', 'Teaching_Hospital_ID']) \
            .select('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name', 'Teaching_Hospital_ID',(col('Total_funding_per_month')/col('Total_funding_per_year'))*100) \
            .withColumnRenamed('((Total_funding_per_month / Total_funding_per_year) * 100)', 'funding_percentage') \
            .filter('funding_percentage >= 90') \
            .write.mode('overwrite').json('/tmp/spark_poc1/query_out/gpay/q2_out')
