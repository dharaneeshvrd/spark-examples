import csv
import datetime
import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import *
from pyspark.sql.types import *

if len(sys.argv) != 6:
    print "Usage Guide: spark-submit <spark-args> dstream.py <bootstrap_servers> <topic_to_read> <topic_to_write> <schema_file> <input_file>"
    exit()

bootstrap_servers = sys.argv[1]
topic_to_read = sys.argv[2]
topic_to_write = sys.argv[3]
schema_file = sys.argv[4]
input_file = sys.argv[5]
schema_str = open(schema_file, 'r').read().strip('\n')
float_fields = [157]
datetime_fields = [158]

def writeInDf(rdd):

    def create_tuple(istr):
        possible_yrs = [2013,2014,2015,2016,2017]
        jl = ()
        for i, e in enumerate(csv.reader(istr.split(','))):
            if i in float_fields+datetime_fields:
                if e:
                    e = e[0]
                    if i in float_fields:
                        jl += (float(e),)
                    elif i in datetime_fields:
                        if '-' in e:
                            tl = e.split('-')
                            (y, m, d) = (int(tl[2]),int(tl[1]),int(tl[0]))
                        else:
                            tl = e.strip('"').split('/')
                            (y, m, d) = (int(tl[2]),int(tl[0]),int(tl[1]))
                        if y not in possible_yrs:
                            (y,m,d) = (2015,1,1)
                        jl += (datetime.datetime(y,m,d),)
                else:
                    jl += (None,)
            elif e:
                jl += (e[0],)
            else:
                jl += (str(),)
        return jl

    mapRdd = rdd.map(lambda x: x[1])

    headerLessRdd = mapRdd.filter(lambda x: schema_str not in x)

    tupleRdd = headerLessRdd.map(create_tuple)
    
    df = spark.createDataFrame(tupleRdd, schema)

    df.selectExpr("to_json(struct(*)) AS value").write.format("kafka").option("kafka.bootstrap.servers", bootstrap_servers).option("topic", topic_to_write).save()

spark = None

def main():
    global spark
    global schema

    # Creating spark contexts and session
    sc = SparkContext(conf=SparkConf().setAppName("DStreamToDF"))
    ssc = StreamingContext(sc, 30)
    spark = SparkSession.builder.appName("DStreamToDF").getOrCreate()

    # Schema preparation
    headers = schema_str.split(',')
    fields = [StructField(ele, StringType(), True) for ele in headers]
    fields[157].dataType = FloatType()
    fields[158].dataType = TimestampType()

    schema = StructType(fields)

    # Define the read stream
    DStream = KafkaUtils.createDirectStream(ssc, [topic_to_read], {"metadata.broker.list": bootstrap_servers})

    # Apply write on rdd's after structuring them
    DStream.foreachRDD(writeInDf)

    ssc.start()
    ssc.awaitTermination()

if __name__ == '__main__':
    main()
