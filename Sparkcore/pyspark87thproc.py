#storing xml data after prpcessing into hive
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").enableHiveSupport().getOrCreate()
sc = spark.sparkContext
#data="C:\\bigdata\\DATASETS\\books.xml"
data="s3://chinmay2022/xmldata/books.xml"
odata="s3://chinmay2022/xmldataoutput/books"
tab="books"
#odata1="E:\\outputofxml\intocsv"
df=spark.read.format('xml').option('rowTag','book').load(data)
res=df.withColumn("processsing-time",current_timestamp()).withColumnRenamed('_id','id').where(col('price')>=30)
res.write.format('hive').option('path',odata).saveAsTable(tab)
print("success!!")
#storing into csv
#res.write.format('csv').option('header','true').option('inferSchema','true').save(odata1)
#print("csv stored!!")

