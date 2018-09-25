from __future__ import division
from pyspark import sql, SparkConf, SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType, DoubleType
import math as M

conf = SparkConf().setAppName("BigData")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

#FORMATO: (user, interest, score)
df_user_interest = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/user_interest_score.csv")
df_user_interest = df_user_interest.withColumn("_c2", df_user_interest["_c2"].cast(IntegerType()))

#FORMATO: (clique, count_user, interest, avg_score, count_user_interest)
df_comm_interest_score = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/clique_interest_score.csv")
df_comm_interest_score = df_comm_interest_score.withColumn("_c3", df_comm_interest_score["_c3"].cast(DoubleType()))

df_user_interest = df_user_interest.rdd \
	.filter(lambda x: x[2]>=900) \
	.map(lambda x: (x[1], (x[0],x[2]) ))

df_comm_interest_score = df_comm_interest_score.rdd \
	.map(lambda x: (x[2],(x[0], x[3], x[1]) ))

df_link = df_user_interest.join(df_comm_interest_score) \
	.map(lambda (a,(b,c)): ( (c[0],b[0]),((b[1]/(M.fabs(c[1]-b[1])+1)), c[2] ) ))


#FORMATO: (clique, user, count_friends)
df_comm = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/new_link.csv")

df_comm = df_comm.rdd \
	.filter(lambda x: x[2] >= 4) \
	.map(lambda x: ((x[0],x[1]),x[2]) )

df = df_link.join(df_comm) \
	.map(lambda (a,(s,c)): ((a[0], a[1], s[1], c), s[0]) ) \
	.reduceByKey(max) \
	.map(lambda (a,b): (a[0], a[1], a[2], a[3], b))

df.toDF().printSchema()
df.toDF().show(10, False)

filepath = "/home/giulia/Documenti/BigData/Community_Prediction/final_analysis/"
df.toDF().write.format("csv").save(filepath)
print('DONE')


