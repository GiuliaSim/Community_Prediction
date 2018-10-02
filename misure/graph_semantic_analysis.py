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



conf = SparkConf().setAppName("BigData")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

#FORMATO: (clique, user, similarity, count_users, score, avg_score) 
df_clique = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/semantic_analysis_filter_all.csv")

df_clique = df_clique.withColumn("_c2", df_clique["_c2"].cast(DoubleType()))
#df_clique = df_clique.withColumn("_c3", df_clique["_c3"].cast(IntegerType()))
df_clique = df_clique.withColumn("_c4", df_clique["_c4"].cast(IntegerType()))
df_clique = df_clique.withColumn("_c5", df_clique["_c5"].cast(DoubleType()))

w = Window.partitionBy('_c0', '_c1')
w2 = Window.partitionBy('_c0')
#FORMATO: (clique, user, similarity, score, avg_score, count_interests, avg_clique) 
df_clique = df_clique.select('_c0', '_c1', '_c2', '_c4', '_c5', count('_c1').over(w).alias('_c6'), avg('_c5').over(w).alias('_c7'))
#df_clique.show(20,False)
#df_clique.describe("_c2","_c4","_c5","_c6","_c7").show()

#Calcola il numero dei link rispetto agli interessi comuni tra clique e utente (da 1 a 5)
#df_clique.rdd.map(lambda x: ((x[0],x[1]),1)).reduceByKey(lambda a,b: a+b) \
#	.map(lambda x : (x[1],1)).reduceByKey(lambda a,b: a+b).toDF().show()

#Calcola la media dello score degli utenti per numero di interessi condivisi separati per intervalli di avg_score delle community
df_clique_score = df_clique.rdd.map(lambda x: ((x[5], getInterval(x[6])),(x[3],1)) ) \
	.reduceByKey(lambda  a,b: (a[0]+b[0],a[1]+b[1]) ) \
	.map(lambda (a,b): (a[0],a[1],b[0]/b[1]) ).toDF().show()

#Calcola la media del similarity score per numero di interessi condivisi separati per intervalli di avg_score delle community
df_clique_similarity = df_clique.rdd.map(lambda x: ((x[5], getInterval(x[6])),(x[2],1)) ) \
	.reduceByKey(lambda  a,b: (a[0]+b[0],a[1]+b[1]) ) \
	.map(lambda (a,b): (a[0],a[1],b[0]/b[1]) ).toDF().show()





	