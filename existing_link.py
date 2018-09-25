from pyspark import sql, SparkConf, SparkContext
from pyspark.sql.functions import avg, col, count, desc, asc
from pyspark.sql.types import IntegerType, FloatType, DoubleType

conf = SparkConf().setAppName("BigData")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

def main(inputFile, outputFile):
	#FORMATO: (community, user, count_friends)
	df_link = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/topological_analysis.csv")

	df_link = df_link.rdd \
		.map(lambda x: ((x[0], x[1]),x[2]))

	#FORMATO: (community, count_user, user)
	df_comm = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/" + inputFile)

	df_comm = df_comm.rdd \
	.map(lambda x: ((x[0], x[2]),x[1]))

	df = df_link.join(df_comm) \
		.map(lambda (a, (b,c)): (a[0], a[1], b))

	df.toDF().printSchema()
	df.toDF().show(10,False)
	#print("_________________________COUNT: ",df.count())

	filepath = "/home/giulia/Documenti/BigData/Community_Prediction/topological_analysis/" + outputFile
	df.toDF().write.format("csv").save(filepath)
	print('DONE')

#main("comm_rej_user.csv","rejected_link/")
main("comm_user.csv","valid_link/")

