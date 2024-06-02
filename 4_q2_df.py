#!/usr/bin/env -S ${HOME}/lib/python/bin/python

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window

import datetime


spark = SparkSession \
    .builder \
    .appName('4_q2_df') \
    .getOrCreate()

spark.catalog.clearCache()


print('Reading csv')
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
# print(df.count())

# df = df.withColumn('TIME OCC', df['TIME OCC'].cast('int'))


# Find all commited in the streets.
filtered = df.filter(df['Premis Desc'] == 'STREET')
# print(filtered.count())

# filtered.write.csv('df.csv', header=False, mode="overwrite")
# exit(0)

@f.udf
def find_day_segment(s):
    time = int(s)
    if time >= 500 and time < 1200:
        return 'morning'
    elif time >= 1200 and time < 1700:
        return 'afternoon'
    elif time >= 1700 and time < 2100:
        return 'evening'
    else:
        return 'night'

segments = filtered.withColumn('day_seg', find_day_segment('TIME OCC'))
# print(segments.count())

counts = segments.groupBy('day_seg').count()
# print(counts.count())

counts.sort([f.desc('count')]).show()

