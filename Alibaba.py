import pyspark
from pyspark import SparkContext 
from pyspark.sql import SQLContext 
import pyspark.sql.functions as f 
from pyspark.sql.window import *
import sched, time

sc = SparkContext("local", "Alibaba2")
sqlContext = SQLContext(sc)

start = 0
def runAnalytics(df):
  global start
  print(start)
  slice = df.where(f.col('index').between(start, ( start + 500 ) ))
  slice.select('Product_name','Product_Company','Product_Rating').orderBy(f.col('Product_Rating'),ascending=False).show(10)  
  slice.select('Product_name','Product_Company','Product_Raters').orderBy(f.col('Product_Raters'),ascending=False).show(10)
  start = start + 500

companies = sqlContext.read.format("csv").option("header", "true").option("inferSchema", "true").load("file:///root/finalFYP.csv")
all = companies.withColumn('index', monotonically_increasing_id())


s = sched.scheduler(time.time, time.sleep)

while True:  
  s.enter(120, 10, runAnalytics, argument=(all,))
  s.run()

sc.stop()


