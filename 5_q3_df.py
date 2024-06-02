#!/usr/bin/env -S ${HOME}/lib/python/bin/python

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.window import Window

import datetime


spark = SparkSession \
    .builder \
    .appName('5_q3_df') \
    .getOrCreate()

spark.catalog.clearCache()


descent_description = [ 
    ['A' , 'Other Asian'],
    ['B' , 'Black'],
    ['C' , 'Chinese'],
    ['D' , 'Cambodian'],
    ['F' , 'Filipino'],
    ['G' , 'Guamanian'],
    ['H' , 'Hispanic/Latin/Mexican'],
    ['I' , 'American Indian/Alaskan Native'],
    ['J' , 'Japanese'],
    ['K' , 'Korean'],
    ['L' , 'Laotian'],
    ['O' , 'Other'],
    ['P' , 'Pacific Islander'],
    ['S' , 'Samoan'],
    ['U' , 'Hawaiian'],
    ['V' , 'Vietnamese'],
    ['W' , 'White'],
    ['X' , 'Unknown'],
    ['Z' , 'Asian Indian'],
]

schema = StructType([       
    StructField('Vict Descent', StringType(), True),
    StructField('Vict Descent text', StringType(), True),
])

df_desc_discr = spark.createDataFrame(data=descent_description, schema=schema)


# "Zip Code","Community","Estimated Median Income"
print('Reading income csv')
df_income = spark.read.format('csv') \
            .options(header='true') \
            .load('hdfs://master:9000/user/osboxes/Median_Household_Income_by_Zip_Code-Los_Angeles_County/income/LA_income_2015.csv')

@f.udf(returnType=IntegerType())
def cast_dollars(s):
    # Remove dollar sign.
    s = s[1:]
    # Remove thousands separator.
    s = s.replace(',', '')
    return int(s)

df_income = df_income.withColumn('Estimated Median Income', cast_dollars('Estimated Median Income'))

# Filter only the city of Los Angeles.
@f.udf(returnType=BooleanType())
def find_la(s):
    return 'Los Angeles' in s

df_income = df_income.filter(find_la('Community'))


df_income = df_income.sort([f.desc('Estimated Median Income')])


# head, tail return a list of Rows, which are dictionaries.
df_income_highest3 = spark.createDataFrame(df_income.head(3))
df_income_lowest3 = spark.createDataFrame(df_income.tail(3))

# head, tail return a list of Rows, which are dictionaries.
# rows_income_highest3 = df_income.head(3)
# rows_income_lowest3 = df_income.tail(3)
# zip_income_highest = [ x['Zip Code'] for x in rows_income_highest3 ]
# zip_income_lowest3 = [ x['Zip Code'] for x in rows_income_lowest3 ]



print('Reading reverse geocoding csv')
df_revgeo = spark.read.format('csv') \
            .options(header='true') \
            .load('hdfs://master:9000/user/osboxes/Median_Household_Income_by_Zip_Code-Los_Angeles_County/revgeocoding.csv')

df_revgeo = df_revgeo.withColumnRenamed('ZIPcode', 'Zip Code')

# Drop duplicate lat,lon pairs, keep any one.
# NONE FOUND!
df_revgeo = df_revgeo.withColumn('latlon', f.udf(lambda x, y: x + ' ' + y)('LAT', 'LON'))
df_revgeo = df_revgeo.dropDuplicates(['latlon'])

# default: BroadcastHashJoin
df_revgeo_h = df_revgeo.join(df_income_highest3, ['Zip Code'], how='inner')
df_revgeo_h.explain()
df_revgeo_h = df_revgeo_h.select(['Zip Code', 'LAT', 'LON'])

# default: BroadcastHashJoin
df_revgeo_l = df_revgeo.join(df_income_lowest3, ['Zip Code'], how='inner')
df_revgeo_l.explain()
df_revgeo_l = df_revgeo_l.select(['Zip Code', 'LAT', 'LON'])


print('Reading data csv')
df = spark.read.format('csv') \
            .options(header='true') \
            .load('hdfs://master:9000/user/osboxes/crime_data/Crime_Data_from_2010_to_2019.csv')

# df = df.withColumn('DATE OCC', f.to_timestamp(df['DATE OCC'], 'MM/dd/yyyy hh:mm:ss a'))
# df = df.withColumn('TIME OCC', df['TIME OCC'].cast('int'))

# df = df.withColumn('year', f.year('DATE OCC'))


# Find out blank 'Vict Descent'.
df = df.filter(~df['Vict Descent'].isNull())


# highest 3

# default: SortMergeJoin ( BroadcastHashJoin )
df_join_h = df.join(df_revgeo_h, ['LAT', 'LON'], how='inner')
df_join_h.explain()
df_join_h = df_join_h.select(['Zip Code', 'Vict Descent'])

df_join_h = df_join_h.groupBy('Vict Descent').count()
df_join_h = df_join_h.withColumnRenamed('count', 'total victims')

# default: SortMergeJoin( SortMergeJoin ( BroadcastHashJoin ) )
df_join_h = df_join_h.join(df_desc_discr, 'Vict Descent', how='inner')
df_join_h.explain()
df_join_h = df_join_h.select(['Vict Descent text', 'total victims'])

df_join_h = df_join_h.sort(f.desc('total victims'))


# lowest 3

# default: SortMergeJoin ( BroadcastHashJoin )
df_join_l = df.join(df_revgeo_l, ['LAT', 'LON'], how='inner')
df_join_l.explain()
df_join_l = df_join_l.select(['Zip Code', 'Vict Descent'])

df_join_l = df_join_l.groupBy('Vict Descent').count()
df_join_l = df_join_l.withColumnRenamed('count', 'total victims')

# default: SortMergeJoin( SortMergeJoin ( BroadcastHashJoin ) )
df_join_l = df_join_l.join(df_desc_discr, 'Vict Descent', how='inner')
df_join_l.explain()
df_join_l = df_join_l.select(['Vict Descent text', 'total victims'])

df_join_l = df_join_l.sort(f.desc('total victims'))



print('Highest 3')
df_join_h = df_join_h.withColumnRenamed('Vict Descent text', 'victim descent')
df_join_h.show(truncate=False)

print('Lowest 3')
df_join_l = df_join_l.withColumnRenamed('Vict Descent text', 'victim descent')
df_join_l.show(truncate=False)

