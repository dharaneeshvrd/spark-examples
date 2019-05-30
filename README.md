# Spark-Examples


## SparkSQL using medical datasets provided by openpayments
Using SparkSQL, tried the following things
* Converted and stored data from raw csv to compacted parquet files for small storage and efficient performance
  * Deafult partition
  * Year wise partition
* Executed various analytical queries

## Datasets
* The datasets containing transaction (to doctors and hospitals) informations around the globe.<br>
* Totally 5 year dataset is available from 2013 to 2017
* Link to download datasets: https://www.cms.gov/OpenPayments/Explore-the-Data/Dataset-Downloads.html

## SparkStreaming
* Explored DStreams and Structured Stream. Have tried one example for converting unstructured data stream to structured hdfs output using DStreams and Structured streaming
* Used Yahoo's Weather API and Strutured Streaming to calculate every hour's average temperature using Window functions
