from __future__ import division
from pyspark import sql, SparkConf, SparkContext
from pyspark.sql.functions import avg, col, count, desc, asc
from pyspark.sql.types import IntegerType, FloatType, DoubleType

def main(inputFile):
	print("*************************")
	print("*************************")
	print("*************************")
	print("*************************Input file: " + inputFile)

	#FORMATO: (comm, user, count_user, count_friends, score)
	df = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/" + inputFile)

	df = df.withColumn("_c2", df["_c2"].cast(IntegerType()))
	df = df.withColumn("_c3", df["_c3"].cast(IntegerType()))
	df = df.withColumn("_c4", df["_c4"].cast(DoubleType()))


	print("______________________________Degree of membership:")
	df_degree = df.rdd \
		.map(lambda x: ((x[2]-x[3]), (1, x[4])) ) \
		.reduceByKey(lambda a, b : (a[0] + b[0], a[1] + b[1])) \
	 	.map(lambda (a,(b,c)): (a,b,c/b))
	#df_degree = df.rdd \
	#	.map(lambda x: ((x[2]-x[3]), (1, x[4], x[4])) ) \
	#	.reduceByKey(lambda a, b : (a[0] + b[0], a[1] + b[1], max(a[2],b[2]))) \
	 #	.map(lambda (a,(b,c,d)): (a,b,c/b,d))
	df_degree.toDF().select(col("_1").alias("degree"), col("_2").alias("link_count"), col("_3").alias("avg_similarity_score")).show()

	#df.toDF().printSchema()
	#df.toDF().show(10, False)


conf = SparkConf().setAppName("BigData")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

main("final_analysis_filter.csv")
main("final_analysis_rej_filter.csv")
main("final_analysis_valid_filter.csv")