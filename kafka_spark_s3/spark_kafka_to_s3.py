from pyspark.sql import SparkSession

spark = SparkSession \
          .builder \
          .appName("APP") \
          .getOrCreate()

# read data from kafka topic

df = spark\
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "b-1.xxxxxxxx.xxxxxx.xx.kafka.us-west-2.amazonaws.com:9092") \
      .option("subscribe", "MSKTutorialTopic") \
      .option("startingOffsets", "earliest") \
      .load()

# write data to s3

query = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("csv") \
    .option("path", "s3://yourbucket/yourfolder/") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
