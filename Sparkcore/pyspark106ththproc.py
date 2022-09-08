from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext

#we need to get these first
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAY4RWHOEUSR2YQD6W")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")

sfOptions = {
  "sfURL" : "st20210.ap-southeast-1.snowflakecomputing.com",
  "sfUser" : "CHINMAYNAIK27",
  "sfPassword" : "Chinmay123",
  "sfDatabase" : "chinmaydb",
  "sfSchema" : "Public",
  "sfWarehouse" : "DEMO1"
}


df=spark.read.format("net.snowflake.spark.snowflake").option("dbtable","banktab").option(**sfOptions).load()
df.show()