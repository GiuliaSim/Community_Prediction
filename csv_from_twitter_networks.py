from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, avg, col, count

spark = SparkSession \
	.builder \
   	.appName("BigData") \
   	.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/BigData.twitternetworks") \
   	.getOrCreate()

df_network = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

df = df_network.select("id", "friends", "friends_count").toDF("user_id", "friends", "friends_count")

df.printSchema()

df = df.rdd \
	.flatMap(lambda x: [(x.user_id, friend, x.friends_count) for friend in x.friends])

df.toDF().printSchema()
df.toDF().show(10, False)


filepath = "/home/giulia/Documenti/BigData/Community_Prediction/data_twitter_networks" 
df.toDF().write.format("csv").save(filepath)

print('Cartella creata: ' + filepath)

