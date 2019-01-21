## Example for converting unstructured stream data to structured data and store it in HDFS for future analysis

### Usage Guide:

**Setup:**
- Download the medical dataset from this link http://download.cms.gov/openpayments/PGYR17_P011819.ZIP
- Unzip and keep only "OP_DTL_RSRCH_PGYR2017_P06292018.csv" this csv file
- Create a *schema.txt* file by copying the first line of the csv file

**Dataflow between scripts:**
- Will read the plain csv text file line by line and write it to a specific Kafka topic
- Use dstream to make it to a structured spark dataframe and convert it to a json key value pair and write it to another Kafka topic
- Use structured stream to consume from kafka topic which is written by dstream and we can apply SQL analysis operation or building ML model using the structured stream data. For example purpose data received from the kafka topic will be straight away written to HDFS using time partition where time is taken from kafka

**Running scripts**
 - Run spark scripts first and then run the writetokafka py file
 
      ***dstream***
      
      *Usage Guide: spark-submit <spark_args> dstream.py <bootstrap_servers> <topic_to_read> <topic_to_write> <schema_file>*
 
      **e.g.:**
      
      *spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0,org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 dstream.py 10.81.1.158:9092 new1 new2 schema.txt*
      
      - Which will read plain csv data from topic *new1* and make it structured key value pair and write it to topic *new2*

      **Note:** Both sql and streaming packages required
      
      ***struct_stream***
      
      *Usage Guide: spark-submit <spark_args> struct_stream.py <bootstrap_servers> <topic_to_read> <schema_file> <output_path> <checkpoint_directory>*
      
      **e.g.:**
      
      spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 struct_stream.py 10.81.1.158:9092 new2 /tmp/spark_poc/out /tmp/spark_poc/checkin
      
      - Which will read structured key value pair from the topic *new2* and write it into */tmp/spark_poc/out* HDFS directory
      
      ***writetokafka***
      
      *Usage Guide: python writetokafka.py <bootstrap_servers> <topic_to_read> <file_to_read>*
      
      **e.g.:**
      
      *python writetokafka.py 10.81.1.158:9092 new1 OP_DTL_RSRCH_PGYR2017_P06292018.csv*
      
      - Which will read the plain csv and write it into topic *new1*
