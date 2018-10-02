from __future__ import division
from pyspark import sql, SparkConf, SparkContext
from pyspark.sql.types import IntegerType, FloatType, DoubleType
from heapq import nlargest

conf = SparkConf().setAppName("BigData")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

#FORMATO: (clique, count_users, interest, avg_score, count_u_i)
df_clique = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/clique_interest_score.csv")

df_clique = df_clique.withColumn("_c1", df_clique["_c1"].cast(IntegerType()))
df_clique = df_clique.withColumn("_c3", df_clique["_c3"].cast(DoubleType()))
df_clique = df_clique.withColumn("_c4", df_clique["_c4"].cast(IntegerType()))

#	.sortBy(lambda x: (x[0], x[3]), ascending=False) \
df_clique = df_clique.rdd \
	.map(lambda x: ((x[0], x[1]),(x[0], x[1], x[2],x[3],x[4])) ) \
	.groupByKey() \
	.flatMap(lambda g: nlargest(5, g[1], key=lambda x: x[3]))
	

df_clique.toDF().show(10,False)

filepath = "/home/giulia/Documenti/BigData/Community_Prediction/clique_interest_score_filter5/"
df_clique.toDF().write.format("csv").save(filepath)
print('DONE')

# print("_________________________COUNT: ",df_clique.count())


# count_clique_avg = df_clique.toDF().agg({"_2": "avg"}).collect()[0][0]
# print('_________________________Count interest clique medio: ', count_clique_avg)
# count_clique_max = df_clique.toDF().agg({"_2": "max"}).collect()[0][0]
# print('_________________________Count interest clique massimo: ', count_clique_max)
# count_clique_min = df_clique.toDF().agg({"_2": "min"}).collect()[0][0]
# print('_________________________Count interest clique minimo: ', count_clique_min)

