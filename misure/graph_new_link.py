from __future__ import division
from pyspark import sql, SparkConf, SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType, DoubleType
import math as M


def getInterval(value):
	if(value < 300):
		return 1
	elif(value < 500):
		return 2
	elif(value < 800):
		return 3
	elif(value < 2000):
		return 4
	else:
		return 5

def getInterval1(value):
	if(value == 1):
		return 1
	elif(value < 4):
		return 2
	elif(value < 7):
		return 3
	elif(value < 10):
		return 4
	else:
		return 5

def filterVal(key,value):
	if(key[0] == 1):
		return (value[2] <= 50)
	elif(key[0] == 2):
		return (value[2] <= 100)
	elif(key[0] == 3):
		return (value[2] <= 200)
	elif(key[0] == 4):
		return (value[2] <= 400)
	else:
		return (value[2] <= 600)

conf = SparkConf().setAppName("BigData")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

#FORMATO: (clique, user, count_users, count_friends, score, avg_score) 
df_new = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/final_analysis.csv")

df_new = df_new.withColumn("_c2", df_new["_c2"].cast(IntegerType()))
df_new = df_new.withColumn("_c3", df_new["_c3"].cast(IntegerType()))
df_new = df_new.withColumn("_c4", df_new["_c4"].cast(DoubleType()))
df_new = df_new.withColumn("_c5", df_new["_c5"].cast(DoubleType()))

df_new = df_new.rdd \
	.filter(lambda x: x[5]>= 100) \
	.map(lambda x: ((getInterval(x[5]),getInterval1(x[2]-x[3])),(x[5],x[4],M.fabs(x[4]-x[5]),1))) \
	.sortBy(lambda (a,b): (a[0], a[1], b[2]) ) \
	.filter(lambda (a,b): filterVal(a,b)) \
	.reduceByKey(lambda a,b: a+b ) \
	.toDF().show()
