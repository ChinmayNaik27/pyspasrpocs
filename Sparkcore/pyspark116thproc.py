from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
#getting data from rest api into a folder andstoring it to mysql server (structured streaming)
#in  this case we are using nifi to get data from rest api to folder and data is in json
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark=SparkSession.builder.master('local[3]').config("spark.streaming.stopGracefullyOnShutDown","true")\
    .config("spark.sql.streaming.schemaInference","true").appName("jdbc-restapi-processing").getOrCreate()

def read_nested_json(df):
    col_lst=[]
    for col_name in df.schema.names:
        if isinstance(df.schema[col_name].dataType,ArrayType):
            df=df.withColumn(col_name,explode(col_name))
        elif isinstance(df.schema[col_name].dataType,StructType):
            for fields in df.schema[col_name].dataType.fields:
                col_lst.append(col(col_name+"."+fields.name).alias(col_name+"_"+fields.name))
        else:
            col_lst.append(col_name)
    df.select(col_lst)
    return df

def flat1(df):
    func_flag=True
    while func_flag:
        df=read_nested_json(df)
        func_flag=False

        for name1 in df.schema.names:
            if isinstance(df.schema[name1].dataType,ArrayType):
                func_flag=True
            elif isinstance(df.schema[name1].dataType,StructType):
                func_flag=True

    return df
#function to store in db
def for_each_to_store(df):
    df.write.mode("append").format("jdbc").option("url","jdbc:mysql://mysqldb1.co7gi3agncec.ap-south-1.rds.amazonaws.com:3306/mysqldb")\
        .option("user","myuser").option("password","password").option("dbtable","restapidata").option("driver","com.mysql.jdbc.Driver").save()

#reading data of all files of given path
df=spark.readStream.format("json").option("multiLine","true",).option("maxFilesPerTrigger",2).load("E:/nifi-output")

ndf=flat1(df)
#store data in db
ndf.writeStream.foreachBatch(for_each_to_store).start().awaitTermination()
"""
#to see output in console
ndf.writeStream.format("console").start().outputMode("append").awaitTermination()
"""
