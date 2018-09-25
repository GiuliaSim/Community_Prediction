from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, avg, col, count

spark = SparkSession \
	.builder \
   	.appName("BigData") \
   	.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/BigData.user_infos") \
   	.getOrCreate()

df_user_infos = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

df = df_user_infos.select("user", "info.interests.all").toDF("user_id", "interests")

df.printSchema()

df = df.rdd \
	.filter(lambda x: x.interests is not None) \
	.flatMap(lambda x: [(x.user_id, interest[0], interest[1].score) for interest in x.interests.items()])
	#.flatMap(lambda x: [(x.user_id, interest, x["score"]) for interest in x.interests])

df.toDF().printSchema()
df.toDF().show(10, False)


filepath = "/home/giulia/Documenti/BigData/Community_Prediction/data_user_interest_score" 
df.toDF().write.format("csv").save(filepath)

print('Cartella creata: ' + filepath)

