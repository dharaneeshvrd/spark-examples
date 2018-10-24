from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

import csv
import datetime

conf = SparkConf().setAppName('test_med_analysis')
sc = SparkContext(conf=conf)

spark = SparkSession \
    .builder \
    .appName("Create_Parquet") \
    .getOrCreate()

gpay = sc.textFile('/tmp/spark_poc/OP_DTL_RSRCH_PGYR2014_P06292018_edited_for_spark.csv')

header_str = 'Change_Type,Covered_Recipient_Type,Noncovered_Recipient_Entity_Name,Teaching_Hospital_CCN,Teaching_Hospital_ID,Teaching_Hospital_Name,Physician_Profile_ID,Physician_First_Name,Physician_Middle_Name,Physician_Last_Name,Physician_Name_Suffix,Recipient_Primary_Business_Street_Address_Line1,Recipient_Primary_Business_Street_Address_Line2,Recipient_City,Recipient_State,Recipient_Zip_Code,Recipient_Country,Recipient_Province,Recipient_Postal_Code,Physician_Primary_Type,Physician_Specialty,Physician_License_State_code1,Physician_License_State_code2,Physician_License_State_code3,Physician_License_State_code4,Physician_License_State_code5,Principal_Investigator_1_Profile_ID,Principal_Investigator_1_First_Name,Principal_Investigator_1_Middle_Name,Principal_Investigator_1_Last_Name,Principal_Investigator_1_Name_Suffix,Principal_Investigator_1_Business_Street_Address_Line1,Principal_Investigator_1_Business_Street_Address_Line2,Principal_Investigator_1_City,Principal_Investigator_1_State,Principal_Investigator_1_Zip_Code,Principal_Investigator_1_Country,Principal_Investigator_1_Province,Principal_Investigator_1_Postal_Code,Principal_Investigator_1_Primary_Type,Principal_Investigator_1_Specialty,Principal_Investigator_1_License_State_code1,Principal_Investigator_1_License_State_code2,Principal_Investigator_1_License_State_code3,Principal_Investigator_1_License_State_code4,Principal_Investigator_1_License_State_code5,Principal_Investigator_2_Profile_ID,Principal_Investigator_2_First_Name,Principal_Investigator_2_Middle_Name,Principal_Investigator_2_Last_Name,Principal_Investigator_2_Name_Suffix,Principal_Investigator_2_Business_Street_Address_Line1,Principal_Investigator_2_Business_Street_Address_Line2,Principal_Investigator_2_City,Principal_Investigator_2_State,Principal_Investigator_2_Zip_Code,Principal_Investigator_2_Country,Principal_Investigator_2_Province,Principal_Investigator_2_Postal_Code,Principal_Investigator_2_Primary_Type,Principal_Investigator_2_Specialty,Principal_Investigator_2_License_State_code1,Principal_Investigator_2_License_State_code2,Principal_Investigator_2_License_State_code3,Principal_Investigator_2_License_State_code4,Principal_Investigator_2_License_State_code5,Principal_Investigator_3_Profile_ID,Principal_Investigator_3_First_Name,Principal_Investigator_3_Middle_Name,Principal_Investigator_3_Last_Name,Principal_Investigator_3_Name_Suffix,Principal_Investigator_3_Business_Street_Address_Line1,Principal_Investigator_3_Business_Street_Address_Line2,Principal_Investigator_3_City,Principal_Investigator_3_State,Principal_Investigator_3_Zip_Code,Principal_Investigator_3_Country,Principal_Investigator_3_Province,Principal_Investigator_3_Postal_Code,Principal_Investigator_3_Primary_Type,Principal_Investigator_3_Specialty,Principal_Investigator_3_License_State_code1,Principal_Investigator_3_License_State_code2,Principal_Investigator_3_License_State_code3,Principal_Investigator_3_License_State_code4,Principal_Investigator_3_License_State_code5,Principal_Investigator_4_Profile_ID,Principal_Investigator_4_First_Name,Principal_Investigator_4_Middle_Name,Principal_Investigator_4_Last_Name,Principal_Investigator_4_Name_Suffix,Principal_Investigator_4_Business_Street_Address_Line1,Principal_Investigator_4_Business_Street_Address_Line2,Principal_Investigator_4_City,Principal_Investigator_4_State,Principal_Investigator_4_Zip_Code,Principal_Investigator_4_Country,Principal_Investigator_4_Province,Principal_Investigator_4_Postal_Code,Principal_Investigator_4_Primary_Type,Principal_Investigator_4_Specialty,Principal_Investigator_4_License_State_code1,Principal_Investigator_4_License_State_code2,Principal_Investigator_4_License_State_code3,Principal_Investigator_4_License_State_code4,Principal_Investigator_4_License_State_code5,Principal_Investigator_5_Profile_ID,Principal_Investigator_5_First_Name,Principal_Investigator_5_Middle_Name,Principal_Investigator_5_Last_Name,Principal_Investigator_5_Name_Suffix,Principal_Investigator_5_Business_Street_Address_Line1,Principal_Investigator_5_Business_Street_Address_Line2,Principal_Investigator_5_City,Principal_Investigator_5_State,Principal_Investigator_5_Zip_Code,Principal_Investigator_5_Country,Principal_Investigator_5_Province,Principal_Investigator_5_Postal_Code,Principal_Investigator_5_Primary_Type,Principal_Investigator_5_Specialty,Principal_Investigator_5_License_State_code1,Principal_Investigator_5_License_State_code2,Principal_Investigator_5_License_State_code3,Principal_Investigator_5_License_State_code4,Principal_Investigator_5_License_State_code5,Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name,Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID,Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name,Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State,Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country,Product_Indicator,Name_of_Associated_Covered_Drug_or_Biological1,Name_of_Associated_Covered_Drug_or_Biological2,Name_of_Associated_Covered_Drug_or_Biological3,Name_of_Associated_Covered_Drug_or_Biological4,Name_of_Associated_Covered_Drug_or_Biological5,NDC_of_Associated_Covered_Drug_or_Biological1,NDC_of_Associated_Covered_Drug_or_Biological2,NDC_of_Associated_Covered_Drug_or_Biological3,NDC_of_Associated_Covered_Drug_or_Biological4,NDC_of_Associated_Covered_Drug_or_Biological5,Name_of_Associated_Covered_Device_or_Medical_Supply1,Name_of_Associated_Covered_Device_or_Medical_Supply2,Name_of_Associated_Covered_Device_or_Medical_Supply3,Name_of_Associated_Covered_Device_or_Medical_Supply4,Name_of_Associated_Covered_Device_or_Medical_Supply5,Total_Amount_of_Payment_USDollars,Date_of_Payment,Form_of_Payment_or_Transfer_of_Value,Expenditure_Category1,Expenditure_Category2,Expenditure_Category3,Expenditure_Category4,Expenditure_Category5,Expenditure_Category6,Preclinical_Research_Indicator,Delay_in_Publication_Indicator,Name_of_Study,Dispute_Status_for_Publication,Record_ID,Program_Year,Payment_Publication_Date,ClinicalTrials_Gov_Identifier,Research_Information_Link,Context_of_Research'

headers = header_str.split(',')
fields = [StructField(ele, StringType(), True) for ele in headers]

for ele in [4,6,26,46,66,86,106]:
    fields[ele].dataType = IntegerType()

fields[127].dataType = LongType()
fields[147].dataType = FloatType()
fields[148].dataType = TimestampType()

schema = StructType(fields)

def split_data(istr):
    l = []
    for i, ele in enumerate(csv.reader(istr.split(','))):
        if not ele and i in [4,6,26,46,66,86,106,127,147,148]:
            ele = [None]
        elif not ele:
            ele = [str()]
        l += ele
    return l

def create_tuple(il):
     jl = ()
     for i, e in enumerate(il):
          if i in [4,6,26,46,66,86,106] and e:
              jl += (int(e.strip('"')),)
          elif i in [127] and e:
              jl += (long(e.strip('"')),)
          elif i in [147] and e:
              jl += (float(e.strip('"')),)
          elif i in [148] and e:
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
gpay_df.write.parquet('/tmp/spark_poc/parq_out_rsrch_2014')
