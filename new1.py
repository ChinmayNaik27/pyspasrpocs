from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark=SparkSession.builder.master('local[*]').appName('testname').getOrCreate()
sc=spark.sparkContext
data="C:\\Users\\chinm\\Downloads\\bank-fullCopy.csv"
df=sc.textFile(data)
# res=df.first()
res1=df.map(lambda x:x.replace("\"","")).filter(lambda x:x!=res)
df1=spark.read.csv(res1,header=True,sep=';',inferSchema=True)
df1.show()
# res1=df.first()
# res=df.map(lambda x:x.replace("\"",""))
# res1=res.map(lambda x:x.split(';')).map(lambda x:(x[0],1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[0])
# # print(res3.collect())
# for i in res1.collect():
#     print(i)
# # res2=res.map(lambda x:x.split(',')).toDF(["CustomerID","CompanyName","ContactName","ContactTitle", "Address","City","Region","PostalCode","Country","Phone","Fax"])
# # res2.show()

# newdf=df.select('id','name').subtract(df.select('id','name'))
# newdf.show()


from pyspark.sql import *

spark=SparkSession.builder.master('local[2]').appName('test').getOrCreate()
df=spark.read.format('org.apache.spark.sql.cassandra').option('keyspace','cassdb').option('table','asl').load()
df.show()