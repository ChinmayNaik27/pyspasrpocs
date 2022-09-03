#convetinfg to avro file format
from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\DATASETS\\books.xml"
odata="E:\\outputofxml\intoxml"
odata1="E:\\outputofxml\\intoavro"
df=spark.read.format('xml').option('rowTag','book').load(data)
#never use spl symbols in col-name
res=df.withColumn("processsingtime",current_timestamp()).withColumnRenamed('_id','id').where(col('price')>=30)
df.show()
res.show()
print("success!!")
#storing into avro file format
res.write.format('avro').save(odata1)
print("avro stored!!")