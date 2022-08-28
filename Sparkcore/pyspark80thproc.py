#tasks
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
df=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:/Users/chinm/Downloads/IBM.csv")
df1=spark.read.format('csv').option('header','true').option('inferSchema','true').load("C:/Users/chinm/Downloads/INFY.csv")
df.show()
df1.show()
df.describe().show()
df1.describe().show()