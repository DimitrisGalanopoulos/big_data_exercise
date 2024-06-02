#!/usr/bin/env -S ${HOME}/lib/python/bin/python

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window

import datetime


spark = SparkSession \
    .builder \
    .appName('4_q2_rdd') \
    .getOrCreate()

spark.catalog.clearCache()

sc = spark.sparkContext


def rdd_head(rdd, n):
    print(rdd.count())
    for r in rdd.take(n): print(r)


print('Reading csv')
rdd1 = sc.textFile('hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2010_to_2019.csv')
rdd2 = sc.textFile('hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2020_to_Present.csv')

# 0     1         2        3        4     5         6           7        8      9           10      11       12       13           14        15          16             17          18     19          20       21       22       23       24       25           26  27
# DR_NO,Date Rptd,DATE OCC,TIME OCC,AREA ,AREA NAME,Rpt Dist No,Part 1-2,Crm Cd,Crm Cd Desc,Mocodes,Vict Age,Vict Sex,Vict Descent,Premis Cd,Premis Desc,Weapon Used Cd,Weapon Desc,Status,Status Desc,Crm Cd 1,Crm Cd 2,Crm Cd 3,Crm Cd 4,LOCATION,Cross Street,LAT,LON
header = rdd1.first()
header = header.split(',')
rev_header = {header[i]:i for i in range(len(header))}
# print(rev_header)

# Remove header from both.
rdd1 = rdd1.zipWithIndex().filter(lambda t: t[1] > 0).keys()
rdd2 = rdd2.zipWithIndex().filter(lambda t: t[1] > 0).keys()


rdd = sc.union([rdd1, rdd2])
# rdd_head(rdd, 10)


# There are values in double quotes "" that have commas inside them!
# We need to view them as one value.

def parse_line(line):
    s = line.replace(',"', '"').replace('",', '"')
    tok = s.split('"')
    ret = []
    for i in range(len(tok)):
        s = tok[i]
        if i % 2 == 0:
            ret.extend(s.split(','))
        else:
            ret.extend([s])
    return ret
rdd_tokens = rdd.map(parse_line)
# rdd_head(rdd_tokens, 10)

rdd_filtered = rdd_tokens.filter(lambda row: row[rev_header['Premis Desc']] == 'STREET')
# rdd_filtered = rdd_tokens.filter(lambda row: row[15] == 'STREET')
# rdd_head(rdd_filtered, 10)

# tmp = rdd_tokens.filter(lambda row: row[rev_header['DR_NO']] == '011401303')
# rdd_head(tmp, 10)

# def toCSVLine(data):
    # return ','.join(str(d) for d in data)
# lines = rdd_filtered.map(toCSVLine)
# lines.saveAsTextFile('rdd.csv')


def find_day_segment(row):
    time = int(row[rev_header['TIME OCC']])
    if time >= 500 and time < 1200:
        return ('morning', 1)
    elif time >= 1200 and time < 1700:
        return ('afternoon', 1)
    elif time >= 1700 and time < 2100:
        return ('evening', 1)
    else:
        return ('night', 1)

rdd_day_seg = rdd_filtered.map(find_day_segment)
# rdd_head(rdd_day_seg, 10)


rdd_counts = rdd_day_seg.reduceByKey(lambda x, y: x + y)
# rdd_head(rdd_counts, 10)


rdd_sort = rdd_counts.sortBy(lambda x: x[1], ascending=False)

print(rdd_sort.collect())

