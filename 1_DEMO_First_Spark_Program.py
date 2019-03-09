# -*- coding: utf-8 -*-

#Initialize SparkSession and SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext

#Create a Spark Session
SpSession = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("jlyang_spark") \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max","2") \
    .config("spark.sql.warehouse.dir", "/Users/jlyang/Spark/spark-warehouse")\
    .getOrCreate()

#Get the Spark Context from Spark Session
SpContext = SpSession.sparkContext
print SpContext

#Create an RDD by loading from a file
tweetsRDD = SpContext.textFile("movietweets.csv")
print tweetsRDD

#Action - Count the number of tweets
print tweetsRDD.count()

#show top 5 records
print tweetsRDD.take(5)

#Transform Data - change to upper Case
ucRDD = tweetsRDD.map( lambda x : x.upper() )
print ucRDD.take(5)


