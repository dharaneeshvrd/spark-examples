from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import csv
import time

conf = SparkConf().setAppName('test_med_analysis')
sc = SparkContext(conf=conf)
sqlc = SQLContext(sc)
gpay = sc.textFile('hdfs://jana-hadoop-mgr-1:8020/tmp/OP_DTL_GNRL_PAY.csv')

header = gpay.first()

fields = [StructField(ele, StringType(), True) for ele in header.split(',')]
fields[3].dataType = IntegerType()
fields[5].dataType = IntegerType()
fields[30].dataType = FloatType()
fields[32].dataType = IntegerType()

schema = StructType(fields)

del_header = gpay.filter(lambda l: 'Change_Type' in l)
new_gpay = gpay.subtract(del_header)

def split_data(istr):
	l = []
	for ele in csv.reader(istr.split(',')):
		if not ele:
			ele = ['']
		l += ele
	return l
	
def create_tuple(il):
	jl = ()
	for i, e in enumerate(il):
		if i in [3,5,32] and e:
			jl += (int(e.strip('"')),)
		elif i in [30] and e:
			jl += (float(e.strip('"')),)
		else:
			jl += (e,)
	return jl

	
final_gpay = new_gpay.map(split_data).map(create_tuple)

gpay_df = sqlc.createDataFrame(final_gpay, schema)

st = time.time()
q1_out = gpay_df.groupBy(gpay_df.Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name, gpay_df.Physician_Specialty).agg({'Total_Amount_of_Payment_USDollars': 'sum'}).collect()
print "time taken: ", time.time() - st
