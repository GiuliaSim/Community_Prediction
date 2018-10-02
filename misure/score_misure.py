from __future__ import division
from pyspark import sql, SparkConf, SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType, DoubleType
import math as M

conf = SparkConf().setAppName("BigData")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

df_user = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/user_interest_score.csv")
df_clique = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/clique_interest_score.csv")

df_user = df_user.withColumn("_c2", df_user["_c2"].cast(IntegerType()))
df_clique = df_clique.withColumn("_c3", df_clique["_c3"].cast(DoubleType()))

df_user.printSchema()
df_clique.printSchema()

score_max = df_user.agg({"_c2": "max"}).collect()[0][0]
print('_________________________Score massimo utente: ', score_max)

score_min = df_clique.agg({"_c3": "min"}).collect()[0][0]
print('_________________________Score medio clique minimo: ', score_min)

	