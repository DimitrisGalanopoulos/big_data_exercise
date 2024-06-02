#!/usr/bin/env -S ${HOME}/lib/python/bin/python

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f


spark = SparkSession \
    .builder \
    .appName("convert to parquet") \
    .getOrCreate()


df = spark.read.format('csv') \
            .options(header='true') \
            .load("hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2010_to_2019.csv")
print(df)
df.show()
df.write.parquet("hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2010_to_2019.parquet")

df = spark.read.format('csv') \
            .options(header='true') \
            .load("hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2020_to_Present.csv")
print(df)
df.show()
df.write.parquet("hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2020_to_Present.parquet")

