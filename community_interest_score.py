from __future__ import division
from pyspark import sql, SparkConf, SparkContext
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, FloatType, DoubleType

conf = SparkConf().setAppName("BigData")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

def main(input_file, output_file):
	df_clique = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/" + input_file)
	df_clique = df_clique.withColumn("_c1", df_clique["_c1"].cast(IntegerType()))
	df_user_interest = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/user_interest_score.csv")
	df_user_interest = df_user_interest \
		.filter("_c2 not like '%score%'") \
		.withColumn("_c2", df_user_interest["_c2"].cast(IntegerType()))

	df_clique = df_clique.rdd \
		.map(lambda x: (x[2], (x[0],x[1]) ))

	df_user_interest = df_user_interest.rdd \
		.map(lambda x: (x[0], (x[1],x[2]) ))

	df = df_clique.join(df_user_interest) \
		.map(lambda (a,(b,c)): ((b[0], b[1], c[0]),(c[1], 1))) \
		.reduceByKey(lambda user1, user2 : ((user1[0] + user2[0]), user1[1] + user2[1])) \
	 	.filter(lambda (a,b): b[1] >= (a[1]-2) ) \
	 	.map(lambda (a,b): (a[0],a[1],a[2],b[0]/b[1],b[1])) \
	 	.sortBy(lambda x: x[3], ascending=False)


	df.toDF().show(10, False)
	print(df.toDF().count())

	filepath = "/home/giulia/Documenti/BigData/Community_Prediction/comm_interest_score/" + output_file
	df.toDF().write.format("csv").save(filepath)
	print('DONE')

CLIQUE_REJ_INPUT = "comms_rej_user.csv"
CLIQUE_VALID_INPUT = "comms_user.csv"
CLIQUE_TOTAL_INPUT = "clique_user.csv"
CLIQUE_REJ_OUTPUT = "comms_rejected"
CLIQUE_VALID_OUTPUT = "comms"
CLIQUE_TOTAL_OUTPUT = "clique"

#main(CLIQUE_REJ_INPUT,CLIQUE_REJ_OUTPUT)
#main(CLIQUE_VALID_INPUT,CLIQUE_VALID_OUTPUT)
main(CLIQUE_TOTAL_INPUT,CLIQUE_TOTAL_OUTPUT)
