# Spark-Examples on medical datasets provided by openpayments

Link to download datasets: https://www.cms.gov/OpenPayments/Explore-the-Data/Dataset-Downloads.html


## Datasets
The datasets containing transaction (to doctors and hospitals) informations around the globe.<br>
Totally 5 year dataset is available from 2013 to 2017

## SparkSQL
Using SparkSQL, tried the following things
* Converted and stored data from raw csv to compacted parquet files for small storage and efficient performance
  * Deafult partition
  * Year wise partition
* Executed various analytical queries
