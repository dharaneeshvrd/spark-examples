import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if len(sys.argv) != 6:
    print "Usage Guide: python struct_stream.py <bootstrap_servers> <topic> <schema_file> <output_path> <checkpoint_directory>"
    exit()

bootstrap_servers = sys.argv[1]
topic = sys.argv[2]
schema_file = sys.argv[3]
output_path = sys.argv[4]
checkpoint_directory = sys.argv[5]

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
    schema_str = open(schema_file, 'r').read().strip('\n')

    headers = schema_str.split(',')
    fields = [StructField(ele, StringType(), True) for ele in headers]
    fields[157].dataType = FloatType()
    fields[158].dataType = TimestampType()

    schema = StructType(fields)

    # Transformations to extract the dataframe from value field which is a dumped json
    df1 = df.selectExpr("CAST(value as STRING) as json","timestamp")

    df2 = df1.select(from_json(col("json"), schema=schema).alias("data"),col("timestamp")).select('data.*', 'timestamp')

    # Using the timestamp given by kafka as medical transactions are not live
    df3 = df2.withColumn("year", year(df2.timestamp)) \
             .withColumn("month", month(df2.timestamp)) \
             .withColumn("day", dayofmonth(df2.timestamp)) \
             .withColumn("hour", hour(df2.timestamp)) \
             .withColumn("minute", minute(df2.timestamp))

    # Writing to hdfs with time partition
    df3.writeStream \
       .outputMode("append") \
       .format("orc") \
       .partitionBy("year", "month", "day", "hour", "minute") \
       .option("checkpointLocation", checkpoint_directory) \
       .option("path", output_path) \
       .start() \
       .awaitTermination()

if __name__ == "__main__":
    main()
