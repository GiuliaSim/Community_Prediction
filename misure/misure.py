from __future__ import division
from pyspark import sql, SparkConf, SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType, DoubleType
import math as M

def main(inputFile):
	print("_________________________")
	print("_________________________")
	print("_________________________")
	print("_________________________Input file: " + inputFile)

	df = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/" + inputFile)

	df = df.withColumn("_c2", df["_c2"].cast(IntegerType()))
	df = df.withColumn("_c3", df["_c3"].cast(IntegerType()))
	df = df.withColumn("_c4", df["_c4"].cast(DoubleType()))

	#df.printSchema()
	#df.show(10, False)

	print("_________________________COUNT: ",df.count())

	score_min = df.agg({"_c4": "min"}).collect()[0][0]
	print('_________________________Score minimo: ', score_min)
	score_max = df.agg({"_c4": "max"}).collect()[0][0]
	print('_________________________Score massimo: ', score_max)
	score_avg = df.agg({"_c4": "avg"}).collect()[0][0]
	print('_________________________Score medio: ', score_avg)

	count_min = df.agg({"_c3": "min"}).collect()[0][0]
	print('_________________________Count common friends minimo: ', count_min)
	count_max = df.agg({"_c3": "max"}).collect()[0][0]
	print('_________________________Count common friends massimo: ', count_max)
	count_avg = df.agg({"_c3": "avg"}).collect()[0][0]
	print('_________________________Count common friends medio: ', count_avg)

	count_clique_min = df.agg({"_c2": "min"}).collect()[0][0]
	print('_________________________Count user clique minimo: ', count_clique_min)
	count_clique_max = df.agg({"_c2": "max"}).collect()[0][0]
	print('_________________________Count user clique massimo: ', count_clique_max)
	count_clique_avg = df.agg({"_c2": "avg"}).collect()[0][0]
	print('_________________________Count user clique medio: ', count_clique_avg)


conf = SparkConf().setAppName("BigData")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

main("final_analysis_norm.csv")
main("final_analysis_rej_norm.csv")
main("final_analysis_valid_norm.csv")