#getting nulls in every columns
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
host='jdbc:redshift://redshift-cluster-1.ct80ntjxksof.ap-south-1.redshift.amazonaws.com:5439/dev'
usr='myuser'
pwd='Password.1'
drv='com.amazon.redshift.jdbc.Driver'
df=spark.read.format('jdbc').option('url',host).option('user',usr).option('password',pwd).option('dbtable','sales').option('driver',drv).load()
#calculating nulls in columns
res=df.select([count(when(col(i).isNull(),i)).alias(i) for i in df.columns])
res.show()