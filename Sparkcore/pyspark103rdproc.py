#writing data to hbase after processing
from pyspark.sql import *
from pyspark.sql.functions import *

spark=SparkSession.builder.master('local[2]').appName('test1').getOrCreate()

df=spark.read.format('org.apache.phoenix.spark').option('table','EMP').option('zkUrl','localhost:2181').load()
df.createOrReplaceTempView("tab")
res=spark.sql("select * from tab where ID=3")
res.show()
df.show()
#the table should be present earlier to process it is compulsory for no sql db
#to create tables on fly we have to write different code
res.write.mode('overwrite').format("org.apache.phoenix.spark").option('table','newtab').option("zkUrl","localhost:2181").save()
