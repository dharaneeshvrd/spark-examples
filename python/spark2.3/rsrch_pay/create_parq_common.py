import json
import csv
import datetime

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

schema = str()
int_fields = list()
long_fields = list()
float_fields = list()
datetime_fields = list()
input_dataset_path = str()

class CSVtoParquet(object):
    def __init__(self, sc, spark_session, schema_info):
        self.sc = sc
        self.spark = spark_session

        global schema
        global int_fields
        global long_fields
        global float_fields
        global datetime_fields
        global input_dataset_path
    
        schema = schema_info['schema']
        int_fields = schema_info['int_fields']
        long_fields = schema_info['long_fields']
        float_fields = schema_info['float_fields']
        datetime_fields = schema_info['datetime_fields']
        input_dataset_path = schema_info['input_dataset_path']

    def prepare_schema(self):
        headers = schema.split(',')
        fields = [StructField(ele, StringType(), True) for ele in headers]

        for ele in int_fields:
            fields[ele].dataType = IntegerType()

        for ele in long_fields:
            fields[ele].dataType = LongType()

        for ele in float_fields:
            fields[ele].dataType = FloatType()

        for ele in datetime_fields:
            fields[ele].dataType = TimestampType()

        schema_def = StructType(fields)

        return schema_def

    @staticmethod
    def split_data(istr):
        l = []
        for i, ele in enumerate(csv.reader(istr.split(','))):
            if not ele and i in int_fields+long_fields+float_fields+datetime_fields:
                ele = [None]
            elif not ele:
                ele = [str()]
            l += ele
        return l

    @staticmethod
    def create_tuple(il):
        jl = ()
        for i, e in enumerate(il):
            if i in int_fields and e:
                jl += (int(e.strip('"')),)
            elif i in long_fields and e:
                jl += (long(e.strip('"')),)
            elif i in float_fields and e:
                jl += (float(e.strip('"')),)
            elif i in datetime_fields and e:
                if '-' in e:
                    tl = e.strip('"').split('-')
                    jl += (datetime.datetime(int(tl[2]),int(tl[1]),int(tl[0])),)
                else:
                    tl = e.strip('"').split('/')
                    jl += (datetime.datetime(int(tl[2]),int(tl[0]),int(tl[1])),)
            else:
                jl += (e,)
        return jl

    def convert(self):
        rdd = self.sc.textFile(input_dataset_path)

        final_rdd = rdd.map(CSVtoParquet.split_data).map(CSVtoParquet.create_tuple)
        
        schema_def = self.prepare_schema()

        df = self.spark.createDataFrame(final_rdd, schema_def)

        return df

def harmonize_schemas_and_combine(df_left, df_right):
    left_types = {f.name: f.dataType for f in df_left.schema}
    right_types = {f.name: f.dataType for f in df_right.schema}
    left_fields = set((f.name, f.dataType, f.nullable) for f in df_left.schema)
    right_fields = set((f.name, f.dataType, f.nullable) for f in df_right.schema)

    # First go over left-unique fields
    for l_name, l_type, l_nullable in left_fields.difference(right_fields):
        if l_name in right_types:
            r_type = left_types[l_name]
            if l_type != r_type:
                raise TypeError, "Union failed. Type conflict on field %s. left type %s, right type %s" % (l_name, l_type, r_type)
            else:
                raise TypeError, "Union failed. Nullability conflict on field %s. left nullable %s, right nullable %s"  % (l_name, l_nullable, not(l_nullable))
        df_right = df_right.withColumn(l_name, lit(None).cast(l_type))

    # Now go over right-unique fields
    for r_name, r_type, r_nullable in right_fields.difference(left_fields):
        if r_name in left_types:
            l_type = right_types[r_name]
            if r_type != l_type:
                raise TypeError, "Union failed. Type conflict on field %s. right type %s, left type %s" % (r_name, r_type, l_type)
            else:
                raise TypeError, "Union failed. Nullability conflict on field %s. right nullable %s, left nullable %s" % (r_name, r_nullable, not(r_nullable))
        df_left = df_left.withColumn(r_name, lit(None).cast(r_type))    

    # Make sure columns are in the same order
    df_left = df_left.select(df_right.columns)

    return df_left.union(df_right)

def union_df(df1, df2):
    df1_fields = set((f.name, f.dataType) for f in df1.schema)
    df2_fields = set((f.name, f.dataType) for f in df2.schema)

    for field_name, field_type, in df1_fields.difference(df2_fields):
        df2 = df2.withColumn(field_name, lit(None).cast(field_type))

    for field_name, field_type in df2_fields.difference(df1_fields):
        df1 = df1.withColumn(field_name, lit(None).cast(field_type))

    df1 = df1.select(df2.columns)

    return df1.union(df2)

def main():
    conf = SparkConf().setAppName('test_med_analysis')
    sc = SparkContext(conf=conf)

    spark = SparkSession \
            .builder \
            .appName("Create_Parquet") \
            .getOrCreate()

    with open('schema.json', 'r') as f:
        schema_info = json.load(f)

    dataframes = list()

    for schema in schema_info:
        csvtoparq = CSVtoParquet(sc, spark, schema_info[schema])
        dataframes.append(csvtoparq.convert())

    final_df = dataframes.pop()

    for df in dataframes:
        final_df = union_df(final_df, df)

    final_df.write.partitionBy('Program_Year').mode('overwrite').parquet('/tmp/spark_poc1/yr_part')
        

if __name__ == '__main__':
    main()
