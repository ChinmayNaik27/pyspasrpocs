#reading data from Snowflake
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext


# You might need to set these
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAY4RWHOEUSR2YQD6W")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")

# Set options below



sfOptions = {
  "sfURL" : "st20210.ap-southeast-1.snowflakecomputing.com",
  "sfUser" : "CHINMAYNAIK27",
  "sfPassword" : "Chinmay123",
  "sfDatabase" : "chinmaydb",
  "sfSchema" : "Public",
  "sfWarehouse" : "DEMO1"
}

SF_VAR = "net.snowflake.spark.snowflake"

df = spark.read.format(SF_VAR).options(**sfOptions).option('dbtable','banktab').load()
df.show()
res=df.withColumn("timestamp",current_timestamp())
res.show()
res.write.mode('overwrite').format(SF_VAR).option('dbtable','practice1').options(**sfOptions).save()