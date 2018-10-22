from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

import csv
import datetime
import time

conf = SparkConf().setAppName('test_med_analysis')
sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .appName("Create_Parquet") \
    .getOrCreate()

gpay = sc.textFile('/tmp/spark_poc/OP_DTL_GNRL_PGYR2017_P06292018_edited_for_spark.csv')

header_str = 'Change_Type,Covered_Recipient_Type,Teaching_Hospital_CCN,Teaching_Hospital_ID,Teaching_Hospital_Name,Physician_Profile_ID,Physician_First_Name,Physician_Middle_Name,Physician_Last_Name,Physician_Name_Suffix,Recipient_Primary_Business_Street_Address_Line1,Recipient_Primary_Business_Street_Address_Line2,Recipient_City,Recipient_State,Recipient_Zip_Code,Recipient_Country,Recipient_Province,Recipient_Postal_Code,Physician_Primary_Type,Physician_Specialty,Physician_License_State_code1,Physician_License_State_code2,Physician_License_State_code3,Physician_License_State_code4,Physician_License_State_code5,Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name,Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID,Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State,Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country,Total_Amount_of_Payment_USDollars,Date_of_Payment,Number_of_Payments_Included_in_Total_Amount,Form_of_Payment_or_Transfer_of_Value,Nature_of_Payment_or_Transfer_of_Value,City_of_Travel,State_of_Travel,Country_of_Travel,Physician_Ownership_Indicator,Third_Party_Payment_Recipient_Indicator,Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value,Charity_Indicator,Third_Party_Equals_Covered_Recipient_Indicator,Contextual_Information,Delay_in_Publication_Indicator,Record_ID,Dispute_Status_for_Publication,Related_Product_Indicator,Covered_or_Noncovered_Indicator_1,Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1,Product_Category_or_Therapeutic_Area_1,Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1,Associated_Drug_or_Biological_NDC_1,Covered_or_Noncovered_Indicator_2,Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2,Product_Category_or_Therapeutic_Area_2,Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2,Associated_Drug_or_Biological_NDC_2,Covered_or_Noncovered_Indicator_3,Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3,Product_Category_or_Therapeutic_Area_3,Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3,Associated_Drug_or_Biological_NDC_3,Covered_or_Noncovered_Indicator_4,Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4,Product_Category_or_Therapeutic_Area_4,Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4,Associated_Drug_or_Biological_NDC_4,Covered_or_Noncovered_Indicator_5,Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5,Product_Category_or_Therapeutic_Area_5,Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5,Associated_Drug_or_Biological_NDC_5,Program_Year,Payment_Publication_Date'

headers = header_str.split(',')
fields = [StructField(ele, StringType(), True) for ele in headers]
fields[3].dataType = IntegerType()
fields[5].dataType = IntegerType()
fields[30].dataType = FloatType()
fields[31].dataType = TimestampType()
fields[32].dataType = IntegerType()

schema = StructType(fields)

def split_data(istr):
    l = []
    for i, ele in enumerate(csv.reader(istr.split(','))):
        if not ele and i in [3,5,30,32]:
            ele = [None]
	elif not ele:
	    ele = [str()]
        l += ele
    return l

def create_tuple(il):
     jl = ()
     for i, e in enumerate(il):
          if i in [3,5,32] and e:
              jl += (int(e.strip('"')),)
          elif i in [30] and e:
              jl += (float(e.strip('"')),)
          elif i in [31] and e:
              if '-' in e:
                  tl = e.strip('"').split('-')
                  jl += (datetime.datetime(int(tl[2]),int(tl[1]),int(tl[0])),)
              else:
                  tl = e.strip('"').split('/')
                  jl += (datetime.datetime(int(tl[2]),int(tl[0]),int(tl[1])),)
          else:
              jl += (e,)
     return jl

final_gpay = gpay.map(split_data).map(create_tuple)

gpay_df = spark.createDataFrame(final_gpay, schema)

# Writing the df as parquet with the default partiton
st = time.time()
gpay_df.write.parquet('/tmp/spark_poc/parq_out1')
et = time.time()
print "time taken:", et-st
