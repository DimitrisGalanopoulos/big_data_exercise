#!/usr/bin/env -S ${HOME}/lib/python/bin/python

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import geopy.distance

import datetime


spark = SparkSession \
    .builder \
    .appName('5_q3_df') \
    .getOrCreate()

spark.catalog.clearCache()

# [n]>>> df.select('AREA').distinct().show()
# +----+
# |AREA|
# +----+
# |  07|
# |  15|
# |  11|
# |  01|
# |  16|
# |  18|
# |  17|
# |  09|
# |  05|
# |  19|
# |  08|
# |  03|
# |  02|
# |  06|
# |  20|
# |  10|
# |  12|
# |  04|
# |  13|
# |  21|
# +----+
# only showing top 20 rows

# [i]>>> df_lapdps.select('AREA').distinct().show()
# +----+
# |AREA|
# +----+
# |   7|
# |  15|
# |  11|
# |   3|
# |   8|
# |  16|
# |   5|
# |  18|
# |  17|
# |   6|
# |  19|
# |   9|
# |   1|
# |  20|
# |  10|
# |   4|
# |  12|
# |  13|
# |  14|
# |  21|
# +----+
# only showing top 20 rows


# X,Y,FID,DIVISION,LOCATION,PREC
print('Reading lapdps csv')
df_lapdps = spark.read.format('csv') \
            .options(header='true') \
            .load('hdfs://master:9000/user/osboxes/lapdps.csv')
df_lapdps = df_lapdps.withColumnRenamed('PREC', 'AREA')
# Change column names with same names not used for join.
df_lapdps = df_lapdps.withColumnRenamed('LOCATION', 'LOCATION_PD')

# Change to int, because df has single digits with leading zeros (e.g., 01) and df_lapdps without (1).
df_lapdps = df_lapdps.withColumn('AREA', df_lapdps['AREA'].cast('int'))


print('Reading data csv')
# DR_NO,Date Rptd,DATE OCC,TIME OCC,AREA ,AREA NAME,Rpt Dist No,Part 1-2,Crm Cd,Crm Cd Desc,Mocodes,Vict Age,Vict Sex,Vict Descent,Premis Cd,Premis Desc,Weapon Used Cd,Weapon Desc,Status,Status Desc,Crm Cd 1,Crm Cd 2,Crm Cd 3,Crm Cd 4,LOCATION,Cross Street,LAT,LON
df1 = spark.read.format('csv') \
            .options(header='true') \
            .load('hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2010_to_2019.csv')
# The header of df1 has 'AREA ' instead of 'AREA', they only had one job.
df1 = df1.withColumnRenamed('AREA ', 'AREA')
# DR_NO,Date Rptd,DATE OCC,TIME OCC,AREA,AREA NAME,Rpt Dist No,Part 1-2,Crm Cd,Crm Cd Desc,Mocodes,Vict Age,Vict Sex,Vict Descent,Premis Cd,Premis Desc,Weapon Used Cd,Weapon Desc,Status,Status Desc,Crm Cd 1,Crm Cd 2,Crm Cd 3,Crm Cd 4,LOCATION,Cross Street,LAT,LON
df2 = spark.read.format('csv') \
            .options(header='true') \
            .load('hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2020_to_Present.csv')
df = df1.union(df2)

# Change to int, because df has single digits with leading zeros (e.g., 01) and df_lapdps without (1).
df = df.withColumn('AREA', df['AREA'].cast('int'))

# Find out 'Null Island' from 'AREA NAME'.
# NONE FOUND!
df = df.filter(df['AREA NAME'] != 'Null Island')


# Filter only crimes involving firearms.
# 'like' for SQL LIKE match.
# 'rlike' for extended regex match.
df = df.filter(df['Weapon Used Cd'].rlike('1..'))


df_join = df.join(df_lapdps, ['AREA'], how='inner')


@f.udf(returnType=DoubleType())
def geodesic_udf(lat1, lon1, lat2, lon2):
    return geopy.distance.geodesic((lat1, lon1), (lat2, lon2)).km

# WHY??? X is LON, Y is LAT
df_join = df_join.withColumn('distance', geodesic_udf('LAT', 'LON', 'Y', 'X'))

df_join = df_join.groupBy('DIVISION').agg(f.avg('distance'), f.count('*'))
df_join = df_join.withColumnRenamed('DIVISION', 'division')
df_join = df_join.withColumnRenamed('avg(distance)', 'average_distance')
df_join = df_join.withColumnRenamed('count(1)', 'incidents total')

df_join = df_join.sort(f.desc('incidents total'))

df_join.show(1000)

