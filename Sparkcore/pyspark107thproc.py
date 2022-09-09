#Reading data from aws Redshift
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
host="jdbc:redshift://redshift-cluster-1.ct80ntjxksof.ap-south-1.redshift.amazonaws.com:5439/dev"
usr='myuser'
pwd='Password.1'
driv='com.amazon.redshift.jdbc.Driver'
df=spark.read.format('jdbc').option('url',host).option('user',usr).option('password',pwd).option('dbtable','sales').option('driver',driv).load()
df.show()
# res=df.select([count(when(col(i).isNull(),i)) for i in df.columns])
# res.show()
# res.write.mode('overwrite').format('jdbc').option('url',host).option('user',usr).option('password',pwd).option('dbtable','sales1')\
#     .option('driver','com.amazon.redshift.jdbc.Driver').save()
# res1=spark.read.csv("C:\\Users\\chinm\\Downloads\\bank-full.csv",header=True,inferSchema=True,sep=';')
# res1.createOrReplaceTempView('tab')
# res2=spark.sql("select * from tab limit 5")
# res2.write.mode("overwrite").format('jdbc').option('url',host).option('user',usr).option('password',pwd).option('driver',driv).option('dbtable','banktab1').save()
# res1.show()