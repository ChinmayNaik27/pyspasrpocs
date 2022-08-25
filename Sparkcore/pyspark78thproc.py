#using arguments externally for program in data
from pyspark.sql import *
from pyspark.sql.functions import *
import sys
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data=sys.argv[1]
df=spark.read.csv(data,header=True,inferSchema=True)
df.show()
ndf=df.withColumn("today's dt",current_date())
ndf.show()
#deployment done by spark-subit --master mastername --deploy-mode mode file args
# whenever arguments used execute from terminal of pycharm python terminal