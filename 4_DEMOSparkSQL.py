# -*- coding: utf-8 -*-

#Initialize SparkSession and SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext

#Create a Spark Session
SpSession = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("prabhath") \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max","2") \
    .config("spark.driver.extraClassPath", "/usr/share/java/mysql-connector-java.jar") \
    .config("spark.executor.extraClassPath", "/usr/share/java/mysql-connector-java.jar") \
    .config("spark.sql.warehouse.dir", "/Users/jlyang/Spark/spark-warehouse")\
    .getOrCreate()

#Get the Spark Context from Spark Session
SpContext = SpSession.sparkContext

#............................................................................
##   Working with Data Frames
#............................................................................

#Create a data frame from a JSON file
empDf = SpSession.read.json("customerData.json")
print '------------------'
print empDf.show()
print '------------------'
print empDf.printSchema()

#Do Data Frame queries
empDf.select("name").show()
print '------------------'
empDf.filter(empDf["age"] == 40).show()
print '------------------'
empDf.groupBy("gender").count().show()
print '------------------'
empDf.groupBy("deptid").\
    agg({"salary": "avg", "age": "max"}).show()
print '------------------'

#create a data frame from a list
deptList = [{'name': 'Sales', 'id': "100"},\
     { 'name':'Engineering','id':"200" }]
deptDf = SpSession.createDataFrame(deptList)
deptDf.show()
 
#join the data frames
empDf.join(deptDf, empDf.deptid == deptDf.id).show()
 
#cascading operations
empDf.filter(empDf["age"] >30).join(deptDf, \
        empDf.deptid == deptDf.id).\
        groupBy("deptid").\
        agg({"salary": "avg", "age": "max"}).show()

#............................................................................
##   Creating data frames from RDD
#............................................................................

from pyspark.sql import Row
lines = SpContext.textFile("auto-data.csv") #RDD
#remove the first line
datalines = lines.filter(lambda x: "FUELTYPE" not in x)
datalines.count()

parts = datalines.map(lambda l: l.split(","))
#print parts.collect()
#[...[u'bmw', u'gas', u'std', u'two', u'sedan', u'rwd', u'six', u'182', u'5400', u'16', u'22', u'41315'], [u'mercedes-benz', u'gas', u'std', u'two', u'hardtop', u'rwd', u'eight', u'184', u'4500', u'14', u'16', u'45400'], ...]
autoMap = parts.map(lambda p: Row(make=p[0], body=p[4], hp=int(p[7]))) #Row

#Infer the schema, and register the DataFrame as a table.
autoDf = SpSession.createDataFrame(autoMap)
autoDf.show()
print '------------------'

#............................................................................
##   Creating data frames directly from CSV
#...........................................................................
autoDf1 = SpSession.read.csv("auto-data.csv",header=True)
autoDf1.show()
print '------------------'

#............................................................................
##   Creating and working with Temp Tables
#............................................................................

autoDf.createOrReplaceTempView("autos")
SpSession.sql("select * from autos where hp > 200").show()
print '------------------'

#register a data frame as table and run SQL statements against it
empDf.createOrReplaceTempView("employees")
SpSession.sql("select * from employees where salary > 4000").show()
print '------------------'

#............................................................................
##   Working with Databases
#............................................................................
#Make sure that the spark classpaths are set appropriately in the 
#spark-defaults.conf file to include the driver files
    
demoDf = SpSession.read.format("jdbc").options(
    url="jdbc:mysql://localhost:3306/testpraba1",
    driver = "com.mysql.jdbc.Driver",
    dbtable = "users_userprofile",
    user="root",
    password="ekdo123").load()
demoDf.show()

"""
#to pandas data frame
#spark data frame is distributed across clusters
#pandas data frame is stored in the master node
empPands = empDf.toPandas()
for index, row in empPands.iterrows():
    print(row["salary"])
"""

