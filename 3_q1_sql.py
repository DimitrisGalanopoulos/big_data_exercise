#!/usr/bin/env -S ${HOME}/lib/python/bin/python

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window
# from pyspark.sql.functions import col
# from pyspark.sql.functions import year


spark = SparkSession \
    .builder \
    .appName('3_q1_sql') \
    .getOrCreate()

spark.catalog.clearCache()


csv = 0
# csv = 1

if csv:
    print('Reading csv')
    df1 = spark.read.format('csv') \
                .options(header='true') \
                .load('hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2010_to_2019.csv')
    df2 = spark.read.format('csv') \
                .options(header='true') \
                .load('hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2020_to_Present.csv')
else:
    print('Reading parquet')
    df1 = spark.read.format('parquet') \
                .options(header='true') \
                .load('hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2010_to_2019.parquet')
    df2 = spark.read.format('parquet') \
                .options(header='true') \
                .load('hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2020_to_Present.parquet')

df = df1.union(df2)

df = df.withColumn('Date Rptd', f.to_timestamp(df['Date Rptd'], 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn('DATE OCC', f.to_timestamp(df['DATE OCC'], 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn('TIME OCC', f.to_timestamp(df['TIME OCC'], 'HHmm'))

df = df.withColumn('year', f.year('DATE OCC'))
df = df.withColumn('month', f.month('DATE OCC'))

# print((df.count(), len(df.columns)))
# df.show()

df.registerTempTable("df")

q_counts = "SELECT year, month, COUNT(*) AS crime_total \
            FROM df \
            GROUP BY year, month \
            ORDER BY year ASC, crime_total DESC"
counts = spark.sql(q_counts)
counts.registerTempTable("counts")

q_ranked = "SELECT year, month, crime_total, \
                ROW_NUMBER() OVER( \
                    PARTITION BY year \
                    ORDER BY crime_total DESC) \
                    AS rank \
            FROM counts"
ranked = spark.sql(q_ranked)
ranked.registerTempTable("ranked")


q_filtered = "SELECT * \
            FROM ranked \
            WHERE rank <= 3"
filtered = spark.sql(q_filtered)


# Filter those with rank <= 3.
# filtered = ranked.filter(ranked['rank'] <= 3)

filtered.show()


