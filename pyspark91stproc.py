# #getting data from cassendra db
# from pyspark.sql import *
# from pyspark.sql.functions import *
#
# spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
# sc = spark.sparkContext
#
# """df=spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","cassdb").option('table','asl').load()
# df.show()
# #keyspace means database of cassendra
# df1=spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","cassdb").option('table','emp').load()
# df1.show()
#
# """
# # from pyspark.sql import *
# def date_function(dfv,fmts=("d-M-yyyy","yyyy-d-M","yyyy-M-d","M-yyyy-d","d-MMM-yyyy","yyyy-d-MMM","yyyy-MMM-d","MMM-yyyy-d","d-MMMM-yyyy","yyyy-d-MMMM","yyyy-MMMM-d","MMMM-yyyy-d")):
#     return coalesce(*[to_date(dfv,i) for i in fmts])
# # spark=SparkSession.builder.master('local[2]').appName('tset').getOrCreate()
# path="C:\\Users\\chinm\\Downloads\\pracfileimpuredt1.txt"
# res=spark.read.format('csv').option("header",'true').option("inferSchema",'true').load(path)
# res.show()
# # dt=udf(date_function)
# res3=res.withColumn("new",date_function(col('date')))
# res3.show()

#Sirs code
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[2]").appName("test").config("spark.jars","E:\\bigdata\\spark-3.1.2-bin-hadoop3.2\\jars\\spark-snowflake_2.12-2.11.0-spark_3.1").getOrCreate()

sc=spark.sparkContext
sc._jvm.net.snowflake.spark.snowflake.SnowflakeConnectorUtils.disablePushdownSession(sc._jvm.org.apache.spark.sql.SparkSession.builder().getOrCreate())

# You might need to set these
sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "AKIAY36A2HGBEWSOX2KJ")
sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "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")

sfOptions ={
"sfURL" : "ox54550.ap-south-1.aws.snowflakecomputing.com",
  "sfUser" : "sep102022",
  "sfPassword" : "CCtv@2022",
  "sfDatabase" : "SNOWFLAKE_SAMPLE_DATA",
  "sfSchema" : "TPCH_SF1",
  "sfWarehouse" : "SMALL"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"
#net\snowflake\spark\snowflake
df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("query",  "select * from CUSTOMER")\
  .option("autopushdown", "off") \
  .load()

df.show()
