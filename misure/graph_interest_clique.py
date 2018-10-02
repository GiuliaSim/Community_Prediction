from __future__ import division
from pyspark import sql, SparkConf, SparkContext
from pyspark.sql.types import IntegerType, FloatType, DoubleType
from pyspark.sql.functions import avg, col, count, desc, asc
from pyspark.sql import Window
from heapq import nlargest


def getInterval(value):
	if(value < 20):
		return 1
	elif(value < 40):
		return 2
	elif(value < 66):
		return 3
	elif(value < 100):
		return 4
	else:
		return 5

#min=10 max=324379
def getInterval2(value):
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

#FORMATO: (clique, count_users, interest, avg_score, count_users_interest)
df_clique = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/clique_interest_score.csv")

df_clique = df_clique.withColumn("_c1", df_clique["_c1"].cast(IntegerType()))
df_clique = df_clique.withColumn("_c3", df_clique["_c3"].cast(DoubleType()))
df_clique = df_clique.withColumn("_c4", df_clique["_c4"].cast(IntegerType()))

#mostra il numero di clique per intervallo1 (ovvero per range relativo alla media degli avg_score dei primi 5 interessi di una clique)
count_score_clique = df_clique.rdd \
	.map(lambda x: (x[0],(x[0], x[3])) ) \
	.groupByKey() \
	.flatMap(lambda g: nlargest(3, g[1], key=lambda x: x[1])) \
	.map(lambda x: (x[0],(x[1],1))) \
	.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) \
	.map(lambda (a,b): ((getInterval2(b[0]/b[1])),(1)) ) \
	.reduceByKey(lambda a,b: a+b) \
	.toDF().show()

#count_score_clique.toDF().describe().show()

#count_min = df_clique.toDF().agg({"_2": "min"}).collect()[0][0]
#print('_________________________Count common interest minimo: ', count_min)
#count_max = df_clique.toDF().agg({"_2": "max"}).collect()[0][0]
#print('_________________________Count common interest massimo: ', count_max)
#count_avg = df_clique.toDF().agg({"_2": "avg"}).collect()[0][0]
#print('_________________________Count common interest medio: ', count_avg)

w = Window.partitionBy('_c0')
df_clique = df_clique.select('_c0', '_c1', '_c2', '_c3', '_c4', count('_c0').over(w).alias('_c5'))

#FORMATO: (clique, count_users, interest, avg_score, count_u_i, num_interest, intervallo)
df_clique = df_clique.rdd.map(lambda x: (x[0],x[1],x[2],x[3],x[4],x[5], getInterval(x[5]))).toDF()

df_clique.printSchema()

#mostra il numero di clique per ogni intervallo (ovvero per range di interessi della clique)
df_clique.rdd.map(lambda x: (x[0],x[6])).reduceByKey(lambda a,b: a) \
	.map(lambda x: (x[1],1) ).reduceByKey(lambda a,b: a+b).toDF().show()


print("*************************")
print("*************************")
print("*************************")
print("*************************Tutti gli interessi:")

df_all = df_clique.groupBy("_7").agg({"_4": "avg"})
df_all.show()


print("*************************")
print("*************************")
print("*************************")
print("*************************Selezionando i primi 15 interessi:")
df_clique = df_clique.rdd \
	.map(lambda x: ((x[0], x[1]),(x[0], x[1], x[2],x[3],x[4],x[5],x[6])) ) \
	.groupByKey() \
	.flatMap(lambda g: nlargest(15, g[1], key=lambda x: x[3]))

df_15 = df_clique.toDF().groupBy("_7").agg({"_4": "avg"})
df_15.show()

print("*************************")
print("*************************")
print("*************************")
print("*************************Selezionando i primi 5 interessi:")
df_clique = df_clique \
	.map(lambda x: ((x[0], x[1]),(x[0], x[1], x[2],x[3],x[4],x[5],x[6])) ) \
	.groupByKey() \
	.flatMap(lambda g: nlargest(5, g[1], key=lambda x: x[3]))

df_5 = df_clique.toDF().groupBy("_7").agg({"_4": "avg"})
df_5.show()

print("*************************")
print("*************************")
print("*************************")
print("*************************Selezionando il primo interesse:")
df_clique = df_clique \
	.map(lambda x: ((x[0], x[1]),(x[0], x[1], x[2],x[3],x[4],x[5],x[6])) ) \
	.groupByKey() \
	.flatMap(lambda g: nlargest(1, g[1], key=lambda x: x[3]))

df_1 = df_clique.toDF().groupBy("_7").agg({"_4": "avg"})
df_1.show()

	