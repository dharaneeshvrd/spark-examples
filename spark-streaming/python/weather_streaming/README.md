## Weather Stream Aggregation per Hour

### Info about weather source:
 - Used free plan of https://www.apixu.com/ to get the weather info about a particular location
 - Example Usage in python: https://github.com/apixu/apixu-python
 
### Usage:
Need to run stream to kafka py file and structured stream parallely
 - First run the structured stream spark code
   
   *Usage Guide: spark-submit struct_stream.py <bootstrap_servers> <topic_to_read> <output_path> <checkpoint_directory>*
   
   **e.g.:**
   
   *spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 struct_stream.py 10.81.1.158:9092 weather /tmp/weather_poc/out /tmp/weather_poc/checkin*
 - Second run the weather_to_kafka python file
 
   *Usage Guide: python writetokafka.py <bootstrap_servers> <topic_to_write> <weather_location> <api_key>*
   
   **e.g.:**
   
   *python weather_to_kafka.py 10.81.1.158:9092 weather Bangalore xyzuserkey*
