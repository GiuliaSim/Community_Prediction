from __future__ import division
from pyspark import sql, SparkConf, SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType, DoubleType
import math as M

def main(inputFile, outputFile):
	#FORMATO: (clique, user, count_users, score, avg_score)
	df_similarity = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/semantic_analysis_filter.csv")
	#df_similarity = df_similarity.withColumn("_c2", df_similarity["_c2"].cast(DoubleType()))
	df_similarity = df_similarity.withColumn("_c2", df_similarity["_c2"].cast(IntegerType()))
	df_similarity = df_similarity.withColumn("_c3", df_similarity["_c3"].cast(DoubleType()))
	df_similarity = df_similarity.withColumn("_c4", df_similarity["_c4"].cast(DoubleType()))

	df_similarity = df_similarity.rdd \
		.map(lambda x: ((x[0],x[1]),(x[2],x[3],x[4])))

	#FORMATO: (clique, user, count_friends)
	df_comm = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/" + inputFile)

	df_comm = df_comm.rdd \
		.map(lambda x: ((x[0],x[1]),x[2]) )

	df = df_comm.join(df_similarity) \
		.map(lambda (a,(c,s)): (a[0], a[1], s[0], c, s[1], s[2]) )


	df.toDF().printSchema()
	df.toDF().show(10, False)
	

	filepath = "/home/giulia/Documenti/BigData/Community_Prediction/final_analysis/existing_link/" + outputFile
	df.toDF().write.format("csv").save(filepath)
	print('DONE')



conf = SparkConf().setAppName("BigData")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

main("rejected_link.csv","rejected/")
main("valid_link.csv","valid/")


