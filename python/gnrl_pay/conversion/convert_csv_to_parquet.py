from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

def main():
    spark = SparkSession \
            .builder \
            .appName("Create_Parquet") \
            .getOrCreate()

    df = spark.read.csv('/tmp/spark_poc1/src/schema2/*', header='true', inferSchema='true')

    def _to_date(col, formats=("MM/dd/yyyy", "dd-MM-yyyy")):
        return coalesce(*[to_date(col, f) for f in formats])

    df = df.withColumn('Date_of_Payment_new', _to_date('Date_of_Payment')).drop('Date_of_Payment').withColumnRenamed('Date_of_Payment_new', 'Date_of_Payment')

    df.repartition(10).write.mode('overwrite').parquet('/tmp/spark_poc1/gpay/demo/schema2')
   

if __name__ == '__main__':
    main()
