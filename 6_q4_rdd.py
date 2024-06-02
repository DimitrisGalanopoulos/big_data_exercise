#!/usr/bin/env -S ${HOME}/lib/python/bin/python

from functools import partial
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window
import geopy.distance

import datetime


# join_method = 0
join_method = 1


spark = SparkSession \
    .builder \
    .appName('5_q3_df') \
    .getOrCreate()

spark.catalog.clearCache()

sc = spark.sparkContext


def head(rdd, n=10):
    print(rdd.count())
    for r in rdd.take(n):
        print(r)

def get_column(rdd, col_num):
    return rdd.map(lambda x: x[col_num])


# X,Y,FID,DIVISION,LOCATION,PREC
print('Reading lapdps csv')
lapdps_header = ['X','Y','FID','DIVISION','LOCATION_PD','AREA']
lapdps_rev_header = {lapdps_header[i]:i for i in range(len(lapdps_header))}
print(lapdps_rev_header)

rdd_lapdps = sc.textFile('hdfs://master:9000/user/osboxes/lapdps.csv')
rdd_lapdps = rdd_lapdps.zipWithIndex().filter(lambda t: t[1] > 0).keys()

def parse_line_lapdps(line):
    s = line.replace(',"', '"').replace('",', '"')
    tok = s.split('"')
    ret = []
    for i in range(len(tok)):
        s = tok[i]
        if i % 2 == 0:
            ret.extend(s.split(','))
        else:
            ret.extend([s])
    ret[lapdps_rev_header['AREA']] = int(ret[lapdps_rev_header['AREA']])
    return ret

rdd_lapdps = rdd_lapdps.map(parse_line_lapdps)


# 0     1         2        3        4     5         6           7        8      9           10      11       12       13           14        15          16             17          18     19          20       21       22       23       24       25           26  27
# DR_NO,Date Rptd,DATE OCC,TIME OCC,AREA,AREA NAME,Rpt Dist No,Part 1-2,Crm Cd,Crm Cd Desc,Mocodes,Vict Age,Vict Sex,Vict Descent,Premis Cd,Premis Desc,Weapon Used Cd,Weapon Desc,Status,Status Desc,Crm Cd 1,Crm Cd 2,Crm Cd 3,Crm Cd 4,LOCATION,Cross Street,LAT,LON
print('Reading data csv')
rdd1 = sc.textFile('hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2010_to_2019.csv')
rdd2 = sc.textFile('hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2020_to_Present.csv')

header = ['DR_NO','Date Rptd','DATE OCC','TIME OCC','AREA','AREA NAME','Rpt Dist No','Part 1-2','Crm Cd','Crm Cd Desc','Mocodes','Vict Age','Vict Sex','Vict Descent','Premis Cd','Premis Desc','Weapon Used Cd','Weapon Desc','Status','Status Desc','Crm Cd 1','Crm Cd 2','Crm Cd 3','Crm Cd 4','LOCATION','Cross Street','LAT','LON']
rev_header = {header[i]:i for i in range(len(header))}
# print(rev_header)

# Remove header from both.
rdd1 = rdd1.zipWithIndex().filter(lambda t: t[1] > 0).keys()
rdd2 = rdd2.zipWithIndex().filter(lambda t: t[1] > 0).keys()
rdd = sc.union([rdd1, rdd2])

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
    ret[rev_header['AREA']] = int(ret[rev_header['AREA']])
    weapon = ret[rev_header['Weapon Used Cd']]
    ret[rev_header['Weapon Used Cd']] = 0 if weapon == '' else int(weapon)
    return ret

rdd = rdd.map(parse_line)


# Find out 'Null Island' from 'AREA NAME'.
# NONE FOUND!
rdd = rdd.filter(lambda row: row[rev_header['AREA NAME']] != 'Null Island')


# Filter only crimes involving firearms.
# 'like' for SQL LIKE match.
# 'rlike' for extended regex match.
# 'Weapon Used Cd' like 1xx.
def is_firearm(row):
    id = row[rev_header['Weapon Used Cd']]
    return id < 200 and id >= 100

rdd = rdd.filter(is_firearm)


# Broadcast Join
#
# Init ()
#     if R not exist in local storage then
#         remotely retrieve R
#         partition R into p chunks R1..Rp
#         save R1..Rp to local storage
#     if R < a split of L then
#         HR <-- build a hash table from R1..Rp
#     else
#         HL1..HLp <-- initialize p hash tables for L
# 
# Map (K: null, V: a record from an L split)
#     if HR exist then
#         probe HR with the join column extracted from V
#         for each match r from HR do
#             emit (null, new_record(r, V))
#     else
#         add V to an HLi hashing its join column
# 
# Close ()
#     if HR not exist then
#         for each non-empty HLi do
#             load Ri in memory
#             for each record r in Ri do
#                 probe HLi with r’s join column
#                 for each match l from HLi do
#                     emit (null, new_record(r, l))

# Broadcast Join - Simplified
#
# Init ()
#     if R not exist in local storage then
#         remotely retrieve R
#         save R to local storage
#     HR <-- build a hash table from R
# 
# Map (K: null, V: a record from an L split)
#     probe HR with the join column extracted from V
#     for each match r from HR do
#         emit (null, new_record(r, V))


rdd_lapdps_local = rdd_lapdps.collect()
HR = {}
for row in rdd_lapdps_local:
    HR[row[lapdps_rev_header['AREA']]] = row

def map_broadcast(row):
    ps = HR[row[rev_header['AREA']]]
    return row + ps

rdd_bc = rdd.map(map_broadcast)


# Repartition Join
#
# Map (K: null, V : a record from a split of either R or L)
#     join_key <-- extract the join column from V
#     tagged_record <-- add a tag of either R or L to V
#     emit (join_key, tagged_record)
#
# Reduce (K′: a join key, LIST_V ′: records from R and L with join key K′)
#     create buffers BR and BL for R and L, respectively
#     for each record t in LIST_V ′ do
#         append t to one of the buffers according to its tag
#     for each pair of records (r, l) in BR × BL do
#         emit (null, new_record(r, l))


def rp_map(key_pos, tag, row):
    k = row[key_pos]
    bufl = []
    bufr = []
    if tag == 0:
        bufl = [row]
    elif tag == 1:
        bufr = [row]
    return (k, (tag, bufl, bufr, []))

rdd_rp = sc.union([rdd_lapdps.map(partial(rp_map, lapdps_rev_header['AREA'], 1)),
                   rdd.map(partial(rp_map, rev_header['AREA'], 0))])

# def rp_reduce(t1, t2):
    # tag1, row1, bufl1, bufr1, res1 = t1[0], t1[1], t1[2], t1[3], t1[4]
    # tag2, row2, bufl2, bufr2, res2 = t2[0], t2[1], t2[2], t2[3], t2[4]
    # tag = 2   # A different tag for intermediate results.
    # bufl = bufl1 + bufl2
    # bufr = bufr1 + bufr2
    # res = res1 + res2
    # return (tag, [], [], bufr, res)

# tmp = rdd_rp.reduceByKey(rp_reduce)


def rp_reduce(t1, t2):
    tag1, bufl1, bufr1, res1 = t1[0], t1[1], t1[2], t1[3]
    tag2, bufl2, bufr2, res2 = t2[0], t2[1], t2[2], t2[3]
    tag = 2   # A different tag for intermediate results.
    bufl = bufl1 + bufl2
    bufr = bufr1 + bufr2
    res = res1 + res2
    # Cartesian product, emit each (r, l) pair in 'res'.
    for rowl in bufl1:
        for rowr in bufr2:
            res += [rowl + rowr]
    for rowl in bufl2:
        for rowr in bufr1:
            res += [rowl + rowr]
    return (tag, bufl, bufr, res)

rdd_rp = rdd_rp.reduceByKey(rp_reduce)

rdd_rp = rdd_rp.flatMap(lambda kv: kv[1][3])

# head(rdd_rp)




if join_method == 0:
    rdd = rdd_bc
else:
    rdd = rdd_rp


def calc_dist(row):
    lat1 = row[rev_header['LAT']]
    lon1 = row[rev_header['LON']]
    lat2 = row[len(rev_header) + lapdps_rev_header['Y']]
    lon2 = row[len(rev_header) + lapdps_rev_header['X']]
    dist = geopy.distance.geodesic((lat1, lon1), (lat2, lon2)).km
    division = row[len(rev_header) + lapdps_rev_header['DIVISION']]
    return (division, (division, dist, 1))

rdd = rdd.map(calc_dist)


def calc_sum(row1, row2):
    return [row1[0], row1[1] + row2[1], row1[2] + row2[2]]

rdd = rdd.reduceByKey(calc_sum)


def map_sort(kv):
    k = kv[0]
    v = kv[1]
    division = v[0]
    dist_sum = v[1]
    count = v[2]
    return (count, (division, dist_sum / count, count))

rdd = rdd.map(map_sort)

rdd = rdd.sortByKey(ascending=False)

rdd = rdd.map(lambda kv: kv[1])

out_bc = rdd.collect()
print('divisioon, average_distance, incidents total')
for row in out_bc:
    print(row)

