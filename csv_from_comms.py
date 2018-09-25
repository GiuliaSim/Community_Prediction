from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, avg, col, count

def main(input_conf, output_file):
	spark = SparkSession \
		.builder \
   		.appName("BigData") \
   		.config("spark.mongodb.input.uri", input_conf) \
   		.getOrCreate()

	dfComms = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

	df = dfComms.select("_id", "count", "nodes").toDF("_id", "count_u", "users")

	df.printSchema()

	df = df.rdd.flatMap(lambda x: [(str(x['_id']).replace("Row(oid=u'",'').replace("')",''), x.count_u, user) for user in x.users])

	df.toDF().printSchema()
	df.toDF().show(10, False)

	filepath = "/home/giulia/Documenti/BigData/Community_Prediction/data_clique/" + output_file
	df.toDF().write.format("csv").save(filepath)

	print('Cartella creata: ' + filepath)

INPUT_REJ = "mongodb://127.0.0.1/BigData.rejected_comms"
INPUT_VALID = "mongodb://127.0.0.1/BigData.communities"
INPUT_TOTAL = "mongodb://127.0.0.1/BigData.clique"
OUTPUT_REJ = "comms_rejected"
OUTPUT_VALID = "comms"
OUTPUT_TOTAL = "clique"

#main(INPUT_REJ,OUTPUT_REJ)
#main(INPUT_VALID,OUTPUT_VALID)
main(INPUT_TOTAL,OUTPUT_TOTAL)
