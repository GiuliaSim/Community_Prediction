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
df_comm_interest_score = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/clique_interest_score_filter.csv")
df_comm_interest_score = df_comm_interest_score.withColumn("_c3", df_comm_interest_score["_c3"].cast(DoubleType()))

df_user_interest = df_user_interest.rdd \
	.filter(lambda x: x[2] >= 300) \
	.map(lambda x: (x[1], (x[0],x[2]) ))

df_comm_interest_score = df_comm_interest_score.rdd \
	.map(lambda x: (x[2],(x[0], x[3], x[1]) ))

#Semantic analysis with reduceByKey: viene preso la media dello score (relativo all'utente) e dell'avg_score (relativo alla clique) per ogni link (clique,user)
df = df_user_interest.join(df_comm_interest_score) \
	.map(lambda (a,(b,c)): ((c[0],b[0], c[2]),(b[1],c[1],1) ))\
	.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1], a[2]+b[2])) \
	.map(lambda (a,b): (a[0],a[1],a[2], b[0]/b[2], b[1]/b[2]))

#Semantic analysis all interest: ritorna tutti gli score per ogni interesse che un utente condivide con una clique
#df = df_user_interest.join(df_comm_interest_score) \
#	.map(lambda (a,(b,c)): ( c[0], b[0], c[2] ,b[1], c[1]) )



#Semantic analysis all interest: ritorna il grado di similarita per ogni interesse che un utente condivide con una clique - NO
#df = df_user_interest.join(df_comm_interest_score) \
#	.map(lambda (a,(b,c)): ( c[0],b[0],(b[1]/((999999*M.fabs(c[1]-b[1])/107479102)+1)), c[2] ,b[1], c[1]) )

#Semantic analysis with reduceByKey: viene preso solo il valore di similarita massimo  - NO
#	.map(lambda (a,(b,c)): ( (c[0],b[0]),((b[1]/(M.fabs(c[1]-b[1])+1)), c[2] ,b[1], c[1]) )) \  senza normalizzazione
#df = df_user_interest.join(df_comm_interest_score) \
#	.map(lambda (a,(b,c)): ( (c[0],b[0]),((b[1]/((999999*M.fabs(c[1]-b[1])/107479102)+1)), c[2] ,b[1], c[1]) )) \
#	.reduceByKey(lambda a,b: max(a,b)) \
#	.map(lambda (a,b): (a[0],a[1],b[0],b[1],b[2],b[3]))

#Semantic analysis all interest
#filepath = "/home/giulia/Documenti/BigData/Community_Prediction/semantic_analysis_all/"

#Semantic analysis with reduceByKey
filepath = "/home/giulia/Documenti/BigData/Community_Prediction/semantic_analysis_filter/"

df.toDF().write.format("csv").save(filepath)
print('DONE')