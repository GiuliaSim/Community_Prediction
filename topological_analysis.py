from pyspark import sql, SparkConf, SparkContext
from pyspark.sql.functions import avg, col, count, desc, asc
from pyspark.sql.types import IntegerType, FloatType, DoubleType

conf = SparkConf().setAppName("BigData")
sc = SparkContext(conf=conf)
sqlContext = sql.SQLContext(sc)

#FORMATO: (user, friend, count_friends)
df_twitter_networks = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/twitternetworks.csv")
#df_twitter_networks = df_twitter_networks.withColumn("_c2", df_twitter_networks["_c2"].cast(IntegerType()))

df_twitter_networks = df_twitter_networks.rdd \
	.map(lambda x: (x[1], x[0]))

#FORMATO: (community, count_user, user)
df_clique = sqlContext.read.csv("/home/giulia/Documenti/BigData/Community_Prediction/data/clique_user.csv")
df_clique = df_clique.withColumn("_c1", df_clique["_c1"].cast(IntegerType()))

df_clique = df_clique.rdd \
	.map(lambda x: (x[2], x[0]))

df = df_clique.join(df_twitter_networks) \
	.map(lambda (a,b): ((b[0], b[1]),1) ) \
	.reduceByKey(lambda a,b : a+b) \
	.map(lambda (a,b): (a[0], a[1], b))

df.toDF().printSchema()
df.toDF().show(10,False)
#print("_________________________COUNT: ",df.count())

filepath = "/home/giulia/Documenti/BigData/Community_Prediction/topological_analysis/"
df.toDF().write.format("csv").save(filepath)
print('DONE')

