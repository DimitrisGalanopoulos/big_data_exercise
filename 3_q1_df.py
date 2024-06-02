#!/usr/bin/env -S ${HOME}/lib/python/bin/python

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window
# from pyspark.sql.functions import col
# from pyspark.sql.functions import year


spark = SparkSession \
    .builder \
    .appName('3_q1_df') \
    .getOrCreate()

spark.catalog.clearCache()


# csv = 0
csv = 1

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

# DR_NO , string
# Date Rptd , string
# DATE OCC , string
# TIME OCC , string
# AREA , string
# AREA NAME , string
# Rpt Dist No , string
# Part 1-2 , string
# Crm Cd , string
# Crm Cd Desc , string
# Mocodes , string
# Vict Age , string
# Vict Sex , string
# Vict Descent , string
# Premis Cd , string
# Premis Desc , string
# Weapon Used Cd , string
# Weapon Desc , string
# Status , string
# Status Desc , string
# Crm Cd 1 , string
# Crm Cd 2 , string
# Crm Cd 3 , string
# Crm Cd 4 , string
# LOCATION , string
# Cross Street , string
# LAT , string
# LON , string

# for col in df.dtypes:
    # print(col[0] + ' , ' + col[1])



# [i]>>> df.show(n=1, truncate=False, vertical=True)
# -RECORD 0-------------------------------------------------
#  DR_NO          | 190326475
#  Date Rptd      | 03/01/2020 12:00:00 AM
#  DATE OCC       | 03/01/2020 12:00:00 AM
#  TIME OCC       | 2130
#  AREA           | 07
#  AREA NAME      | Wilshire
#  Rpt Dist No    | 0784
#  Part 1-2       | 1
#  Crm Cd         | 510
#  Crm Cd Desc    | VEHICLE - STOLEN
#  Mocodes        | NULL
#  Vict Age       | 0
#  Vict Sex       | M
#  Vict Descent   | O
#  Premis Cd      | 101
#  Premis Desc    | STREET
#  Weapon Used Cd | NULL
#  Weapon Desc    | NULL
#  Status         | AA
#  Status Desc    | Adult Arrest
#  Crm Cd 1       | 510
#  Crm Cd 2       | 998
#  Crm Cd 3       | NULL
#  Crm Cd 4       | NULL
#  LOCATION       | 1900 S  LONGWOOD                     AV
#  Cross Street   | NULL
#  LAT            | 34.0375
#  LON            | -118.3506
# only showing top 1 row

# df.select(['LAT']).show()

# df.groupBy('name').agg({'age': 'sum'}).sort('name').show()

counts = df.groupBy(['year', 'month']).count().withColumnRenamed('count', 'crime_total')
counts = counts.sort([f.asc('year'), f.desc('crime_total')])

# ranked = counts.withColumn('rank', f.rank().over(Window.partitionBy(['year']).orderBy(f.desc('crime_total'))))

# It wants the position not the rank (matters when there are equalities).
ranked = counts.withColumn('rank', f.row_number().over(Window.partitionBy(['year']).orderBy(f.desc('crime_total'))))

# Filter those with rank <= 3.
filtered = ranked.filter(ranked['rank'] <= 3)

filtered.show()


