#extracting data from complex json data
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\DATASETS\\zips.json"
df=spark.read.format('json').load(data)
df.show()
df.printSchema()
#splitting array elements
res1=df.withColumn("loc",explode(col('loc')))
res1.show()
#storing data into columns
res2=df.withColumn("lattitude",col('loc')[0]).withColumn("longitude",col('loc')[1]).drop('loc').withColumnRenamed("_id","id")
res2.show()
res2.printSchema()