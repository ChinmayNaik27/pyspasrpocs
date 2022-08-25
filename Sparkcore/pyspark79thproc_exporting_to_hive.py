
#exporting data to hive
#this code will run only if hive is present on the system

from pyspark.sql import *
from pyspark.sql.functions import *
import sys
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
#to run code for hive comment above line and uncomment below line
# spark=SparkSession.builder.master('local[*]').appName('test').enableHiveSupport().getOrCreate()
sc = spark.sparkContext
data=sys.argv[1]
# tab=sys.argv[2]
df=spark.read.format('csv').option('header','true').option('inferSchma','true').load(data)
ndf=df.withColumn("New",when(col('city')=='Ashland',"Place Mut Visit").otherwise("not so good"))
ndf.show(truncate=False)
# ndf.write.format('hive').saveAsTable(tab)
"""Note:"""
#deployment done by spark-subit --master mastername --deploy-mode mode file args
# whenever arguments used execute from terminal of pycharm python terminal