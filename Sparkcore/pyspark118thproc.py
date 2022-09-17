#data cleaning (clean data), to filter out / removing / delete emojies/collection of special characters links or websites from given data file
# this is filtering data from twitter
from pyspark.sql import *
from pyspark.sql.functions import *
import datetime as dt
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
df=spark.read.format("csv").option("inferSchema","true").load("C:\\Users\\chinm\\OneDrive\\Desktop\\demo.csv")\
    .withColumnRenamed("_c0","userid").withColumnRenamed("_c1","AccountID").withColumnRenamed("_c2","timestamp")\
    .withColumnRenamed("_c3","loginissue").withColumnRenamed("_c4","Username").withColumnRenamed("_c5","Response")
# df.show()
df.printSchema()
df.show(truncate=False)
#expression to remove links
ex=r'https?:\/\/.*[\r\n]* -'                              #regular expression to filter out links in data
ex1=r'^(\S+)'                                #regular expression to fetch 1st value upto 1st space only
ex2=r' \W+'                                   # regular expression to filter out usernames in data
ex3=r'^ '                               #to remove extra space in start
ex4='[^a-zA-Z0-9\\s.]'                #to remove all special symbols except .,space

#this is done by creating new columns
res=df.withColumn("username1",regexp_replace(col("Response"),ex,""))
res.show(truncate=False)

#on existing frame
res1=df.withColumn("to_whom",regexp_extract(col("Response"),ex1,1))\
    .withColumn("to_whom",regexp_replace(col("to_whom"),"@",""))\
    .withColumn("Response",regexp_replace(col("Response"),ex,""))\
    .withColumn("Response",regexp_replace(col("Response"),ex1,""))\
    .withColumn("Response",regexp_replace(col("Response"),ex2,""))\
    .withColumn("Response",regexp_replace(col("Response"),ex3,""))\
    .withColumn("Response",regexp_replace(col("Response"),ex4,""))\
    .withColumn("Username",regexp_replace(col("Username"),ex4,""))
res1.show(truncate=False)
op=res1.limit(300)                        #to fetch only first 300 records of file
op.write.format("csv").mode("append").option("header","true").save(f"E:\spark-output\\{dt.date.today()}")