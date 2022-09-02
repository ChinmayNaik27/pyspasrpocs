#storing xml data after prpcessing into xmlformat and csv format
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\DATASETS\\books.xml"
odata="E:\\outputofxml\intoxml"
odata1="E:\\outputofxml\intocsv"
df=spark.read.format('xml').option('rowTag','book').load(data)
res=df.withColumn("processsing-time",current_timestamp()).withColumnRenamed('_id','id').where(col('price')>=30)
res.write.format('xml').option('rootTag','main').option('rowTag','book').save(odata)
print("success!!")
#storing into csv
res.write.format('csv').option('header','true').option('inferSchema','true').save(odata1)
print("csv stored!!")