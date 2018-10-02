from __future__ import division
from pyspark import sql, SparkConf, SparkContext
from pyspark.sql.types import IntegerType, FloatType, DoubleType
from pyspark.sql.functions import avg, col, count, desc, asc
from pyspark.sql import Window
from heapq import nlargest


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



conf = SparkConf().setAppName("BigData")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

#FORMATO: (clique, user, count_users, count_friends, score, avg_score) 
df_valid = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/final_analysis_valid.csv")
df_rej = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/final_analysis_rej.csv")
df_new = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/final_analysis.csv")

df_valid = df_valid.withColumn("_c2", df_valid["_c2"].cast(IntegerType()))
df_valid = df_valid.withColumn("_c3", df_valid["_c3"].cast(IntegerType()))
df_valid = df_valid.withColumn("_c4", df_valid["_c4"].cast(DoubleType()))
df_valid = df_valid.withColumn("_c5", df_valid["_c5"].cast(DoubleType()))

df_rej = df_rej.withColumn("_c2", df_rej["_c2"].cast(IntegerType()))
df_rej = df_rej.withColumn("_c3", df_rej["_c3"].cast(IntegerType()))
df_rej = df_rej.withColumn("_c4", df_rej["_c4"].cast(DoubleType()))
df_rej = df_rej.withColumn("_c5", df_rej["_c5"].cast(DoubleType()))

df_new = df_new.withColumn("_c2", df_new["_c2"].cast(IntegerType()))
df_new = df_new.withColumn("_c3", df_new["_c3"].cast(IntegerType()))
df_new = df_new.withColumn("_c4", df_new["_c4"].cast(DoubleType()))
df_new = df_new.withColumn("_c5", df_new["_c5"].cast(DoubleType()))

#w = Window.partitionBy('_c0')

print("*************************")
print("*************************")
print("*************************")
print("*************************Valid:")
#FORMATO: (clique, user, count_users, count_friends, similarity, score, avg_score, avg_clique) 
#df_valid = df_valid.select('_c0', '_c1', '_c2', '_c3', '_c4', '_c5', '_c6', avg('_c6').over(w).alias('_c7'))
#df_valid.show(20,False)
#df_valid.describe("_c2","_c3","_c4","_c5","_c6","_c7").show()

#Calcola numero di link per scarto count_users e count_friends
df_valid1 = df_valid.rdd \
	.map(lambda x: (getInterval1(x[2]-x[3]),1)).reduceByKey(lambda a,b: a+b) \
	.sortBy(lambda x : (x[0]) ) \
	.toDF().show()

#Calcola numero di link per avg_score delle clique
df_valid2 = df_valid.rdd \
	.map(lambda x: (getInterval(x[5]),1)).reduceByKey(lambda a,b: a+b) \
	.sortBy(lambda x : (x[0]) ) \
	.toDF().show()

#Calcola x[4]similarity/x[5]score medio dei link per avg_score delle clique
df_valid3 = df_valid.rdd \
	.map(lambda x: (getInterval(x[5]),(x[4],1) )).reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
	.map(lambda (a,b): (a,b[0]/b[1])) \
	.sortBy(lambda x : (x[0]) ) \
	.toDF().show()

#Calcola x[4]similarity/x[5]score medio dei link per avg_score delle clique e scarto count_users e count_frinds
df_valid4 = df_valid.rdd \
	.map(lambda x: ((getInterval(x[5]),getInterval1(x[2]-x[3])),(x[4],1))).reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
	.map(lambda (a,b): (a[0],a[1],b[0]/b[1],b[1])) \
	.sortBy(lambda x: (x[0],x[1]) ) \
	.toDF().show(25)


print("*************************")
print("*************************")
print("*************************")
print("*************************Rejected:")
#FORMATO: (clique, user, count_users, count_friends, similarity, score, avg_score, avg_clique) 
#df_rej = df_rej.select('_c0', '_c1', '_c2', '_c3', '_c4', '_c5', '_c6', avg('_c6').over(w).alias('_c7'))
#df_rej.show(20,False)
#df_rej.describe("_c2","_c3","_c4","_c5","_c6","_c7").show()

#Calcola numero di link per scarto count_users e count_friends
df_rej1 = df_rej.rdd \
	.map(lambda x: (getInterval1(x[2]-x[3]),1)).reduceByKey(lambda a,b: a+b) \
	.sortBy(lambda x : (x[0]) ) \
	.toDF().show()

#Calcola numero di link per avg_score delle clique
df_rej2 = df_rej.rdd \
	.map(lambda x: (getInterval(x[5]),1)).reduceByKey(lambda a,b: a+b) \
	.sortBy(lambda x : (x[0]) ) \
	.toDF().show()

#Calcola score medio dei link per avg_score delle clique
df_rej3 = df_rej.rdd \
	.map(lambda x: (getInterval(x[5]),(x[4],1) )).reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
	.map(lambda (a,b): (a,b[0]/b[1])) \
	.sortBy(lambda x : (x[0]) ) \
	.toDF().show()

#Calcola score medio dei link per avg_score delle clique e scarto count_users e count_frinds
df_rej4 = df_rej.rdd \
	.map(lambda x: ((getInterval(x[5]),getInterval1(x[2]-x[3])),(x[4],1))).reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
	.map(lambda (a,b): (a[0],a[1],b[0]/b[1],b[1])) \
	.sortBy(lambda x : (x[0],x[1]) ) \
	.toDF().show(25)

print("*************************")
print("*************************")
print("*************************")
print("*************************New:")
#FORMATO: (clique, user, count_users, count_friends, score, avg_score, avg_clique) 
#df_new = df_new.select('_c0', '_c1', '_c2', '_c3', '_c4', '_c5', '_c6', avg('_c6').over(w).alias('_c7'))
#df_new.show(20,False)
#df_new.describe("_c2","_c3","_c4","_c5","_c6","_c7").show()

#Calcola numero di link per scarto count_users e count_friends
df_new1 = df_new.rdd \
	.map(lambda x: (getInterval1(x[2]-x[3]),1)).reduceByKey(lambda a,b: a+b) \
	.sortBy(lambda x : (x[0]) ) \
	.toDF().show()

#Calcola numero di link per avg_score delle clique
df_new2 = df_new.rdd \
	.map(lambda x: (getInterval(x[5]),1)).reduceByKey(lambda a,b: a+b) \
	.sortBy(lambda x : (x[0]) ) \
	.toDF().show()

#Calcola x[4]similarity/x[5]score medio dei link per avg_score delle clique
df_new3 = df_new.rdd \
	.map(lambda x: (getInterval(x[5]),(x[4],1) )).reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
	.map(lambda (a,b): (a,b[0]/b[1])) \
	.sortBy(lambda x : (x[0]) ) \
	.toDF().show()

#Calcola x[4]similarity/x[5]score medio dei link per avg_score delle clique e scarto count_users e count_frinds
df_new4 = df_new.rdd \
	.map(lambda x: ((getInterval(x[5]),getInterval1(x[2]-x[3])),(x[4],1))).reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
	.map(lambda (a,b): (a[0],a[1],b[0]/b[1],b[1])) \
	.sortBy(lambda x : (x[0],x[1]) ) \
	.toDF().show(25)
