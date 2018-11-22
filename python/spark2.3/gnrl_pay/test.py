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


def execute_queries(gpay_df):
    # 1st query execution and writting the result
    gpay_df.filter('Program_Year == 2015').select('Physician_Specialty', 'Recipient_State', 'Total_Amount_of_Payment_USDollars').groupBy('Physician_Specialty', 'Recipient_State').agg({'Total_Amount_of_Payment_USDollars': 'sum'}).withColumnRenamed('sum(Total_Amount_of_Payment_USDollars)', 'Total_funding').orderBy('Total_funding', ascending=False).limit(20).coalesce(1).write.mode('overwrite').json('/tmp/spark_poc1/gpay/q1_out')

    # 2nd query execution and writting the result
    gpay_df.filter('Program_Year == 2015').select(gpay_df.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, gpay_df.Physician_Specialty, gpay_df.Total_Amount_of_Payment_USDollars).groupBy(gpay_df.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, gpay_df.Physician_Specialty).agg({'Total_Amount_of_Payment_USDollars': 'sum'}).withColumnRenamed('sum(Total_Amount_of_Payment_USDollars)', 'Total_funding').coalesce(1).write.mode('overwrite').json('/tmp/spark_poc1/gpay/q2_out')

    # 3rd query execution and writting the result
    gpay_df.filter('Program_Year == 2015').select('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name', 'Teaching_Hospital_Name', 'Program_Year', 'Total_Amount_of_Payment_USDollars').groupBy('Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name', 'Teaching_Hospital_Name', 'Program_Year').agg({'Total_Amount_of_Payment_USDollars': 'sum'}).withColumnRenamed('sum(Total_Amount_of_Payment_USDollars)', 'Total_funding_in_dollars').coalesce(1).write.mode('overwrite').json('/tmp/spark_poc1/gpay/q3_out')

# Loading the compressed dataset into a dataframe
def union_df(df1, df2):
    df1_fields = set((f.name, f.dataType) for f in df1.schema)
    df2_fields = set((f.name, f.dataType) for f in df2.schema)

    for field_name, field_type, in df1_fields.difference(df2_fields):
        df2 = df2.withColumn(field_name, lit(None).cast(field_type))

    for field_name, field_type in df2_fields.difference(df1_fields):
        df1 = df1.withColumn(field_name, lit(None).cast(field_type))

    df1 = df1.select(df2.columns)

    return df1.union(df2)

execute_queries(spark.read.option('mergeSchema', 'true').parquet('/tmp/spark_poc1/gpay/default_part_schema1', '/tmp/spark_poc1/gpay/default_part_schema2'))

df1 = spark.read.parquet('/tmp/spark_poc1/gpay/default_part_schema1')
df2 = spark.read.parquet('/tmp/spark_poc1/gpay/default_part_schema2')

#df3 = union_df(df1, df2)
#temp = df3.count()
#execute_queries(df3)
