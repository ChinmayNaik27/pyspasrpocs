#processing Json data (simple data)
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
data="C:\\bigdata\\DATASETS\\cars.json"
odata="E:\\outputofjson\simplejson"
df=spark.read.format('json').load(data)
df.show()
df.printSchema()

#processing data an dstoring it into lfs

res=df.where(col('price')>=29200).withColumn('last_update_record',current_timestamp())
res.show()
res.printSchema()
res.write.format('csv').option('header','true').save(odata)
print("Task Complete")