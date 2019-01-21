import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if len(sys.argv) != 5:
    print "Usage Guide: python struct_stream.py <bootstrap_servers> <topic> <output_path> <checkpoint_directory>"
    exit()

bootstrap_servers = sys.argv[1]
topic = sys.argv[2]
output_path = sys.argv[3]
checkpoint_directory = sys.argv[4]

def main():
    # Creating the spark session
    spark = SparkSession.builder.appName('Kafka spark structured stream').getOrCreate()

    # Creating kafka structured stream
    df = spark.readStream \
              .format("kafka") \
              .option("kafka.bootstrap.servers", bootstrap_servers) \
              .option("subscribe", topic) \
              .load()

    # Preparing the schema
    fields = list()
    fields.append(StructField('location', StringType(), True))
    fields.append(StructField('temp_c', DoubleType(), True))
    fields.append(StructField('timestamp_s', StringType(), True))

    schema = StructType(fields)

    # Transformations to extract the dataframe from value field, which is a dumped json
    df1 = df.selectExpr("CAST(value as STRING) as json")

    df2 = df1.select(from_json(col("json"), schema=schema).alias("data")).select('data.*')

    # Creating a timestamp type column from epoch string
    df3 = df2.withColumn('timestamp', from_unixtime(col('timestamp_s')).cast('timestamp')).drop('timestamp_s')

    df4 = df3.withColumn('year', year(col('timestamp'))) \
             .withColumn('month', month(col('timestamp'))) \
             .withColumn('day', dayofmonth(col('timestamp')))

    # Applying windowing function to calculate average tempurature for every hour
    df5 = df4.withWatermark("timestamp", "10 minutes") \
             .groupBy(window('timestamp', '1 hour'), 'location', 'year', 'month', 'day') \
             .mean('temp_c') \
             .withColumnRenamed('avg(temp_c)', 'AverageTemperature')

    # Writing to hdfs with time partition
    df5.writeStream \
       .outputMode("append") \
       .format("json") \
       .partitionBy("year", "month", "day") \
       .option("checkpointLocation", checkpoint_directory) \
       .option("path", output_path) \
       .start() \
       .awaitTermination()

if __name__ == "__main__":
    main()
