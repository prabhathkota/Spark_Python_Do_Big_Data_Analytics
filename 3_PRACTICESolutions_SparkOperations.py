"""
-----------------------------------------------------------------------------
# Loading and Storing Data
***************************
1. Your course resource has a CSV file "iris.csv". 
Load that file into an RDD called irisRDD
Cache the RDD and count the number of lines
-----------------------------------------------------------------------------
"""

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
    .config("spark.sql.warehouse.dir", "/Users/jlyang/Spark/spark-warehouse")\
    .getOrCreate()

#Get the Spark Context from Spark Session
SpContext = SpSession.sparkContext

irisRDD = SpContext.textFile("iris.csv")
irisRDD.cache()
print 'iris Data count: {}' . format(irisRDD.count()) #151
print irisRDD.take(5)

print '-------------'

"""
-----------------------------------------------------------------------------
# Spark Transformations
************************
Create a new RDD from irisRDD with the following changes
    - The name of the flower should be all capitals
    - The numeric values should be rounded off (as integers)
"""

#Create a transformation function
def xformIris( irisStr) :
    if ( irisStr.find("Sepal") != -1):
        return irisStr
        
    attList=irisStr.split(",")
    attList[4] = attList[4].upper()
    #Rounding off
    # '5.1,3.5,1.4,0.2,setosa', u'4.9,3,1.4,0.2,setosa'
    # converts to
    # '5.0,4.0,1.0,0.0,SETOSA', u'5.0,3.0,1.0,0.0,SETOSA'
    for i in range(0,4):
        attList[i] = str(round(float(attList[i])))
    
    return ",".join(attList)

xformedIris = irisRDD.map(xformIris)
print xformedIris.take(5)

print '-------------'
"""
-----------------------------------------------------------------------------
 Filter irisRDD for lines that contain "versicolor" and count them.
-----------------------------------------------------------------------------
"""
versiData = irisRDD.filter(lambda x: "versicolor" in x)
print 'Vericolor count: {}' .format(versiData.count())


"""
-----------------------------------------------------------------------------
# Spark Actions
**************** 
 Find the average Sepal.Length for all flowers in the irisRDD

Note: If you just copied/modified the example function, it may not work.
Find out why and fix it.
-----------------------------------------------------------------------------
"""

#Sepal.Length is a float value. So doing any integer operations 
#will not work. You need to use float functions

#function to check if a string has float value or not.
is_float = lambda x: x.replace('.','',1).isdigit() and "." in x

#Function to find the sum of all Sepal.Length values
def getSepalLength( irisStr) :
    
    if isinstance(irisStr, float) :
        return irisStr
        
    attList=irisStr.split(",")
    
    if is_float(attList[0]) :
        return float(attList[0])
    else:
        return 0.0

#Do a reduce to find the sum and then divide by no. of records.        
SepLenAvg=irisRDD.reduce(lambda x,y : getSepalLength(x) + getSepalLength(y)) \
    / (irisRDD.count()-1)
    
print 'Sepal Average Length: {} ' . format(SepLenAvg)

#get header
irisHeader = irisRDD.first()
irisRDDWithoutHeader = irisRDD.filter(lambda x : x != irisHeader)
#print irisRDDWithoutHeader

"""
-----------------------------------------------------------------------------
# Key-Value RDDs
******************
Convert the irisRDD into a key-value RDD with Species as key and Sepal.Length
as the value.

Then find the maximum of Sepal.Length by each Species.

-----------------------------------------------------------------------------
"""

#Create KV RDD 
#setosa:5.1, setosa:4.9, virginica: 7.7
flowerData = irisRDD.map( lambda x: ( x.split(",")[4], \
    x.split(",")[0]))
flowerData.take(5)
#print flowerData.keys().collect()

print '---------------------------------------------'

#Remove header row
header = flowerData.first()
flowerKV= flowerData.filter(lambda line: line != header)
flowerKV.collect()

#find maximum of Sepal.Length by Species
maxData = flowerKV.reduceByKey(lambda x, y: max(float(x),float(y)))
print maxData.collect() #[(u'versicolor', 7.0), (u'setosa', 5.8), (u'virginica', 7.9)]

"""
-----------------------------------------------------------------------------
# Advanced Spark
******************
 Find the number of records in irisRDD, whose Sepal.Length is 
greater than the Average Sepal Length we found in the earlier practice

Note: Use Broadcast and Accumulator variables for this practice
-----------------------------------------------------------------------------
"""

#Initialize accumulator
sepalHighCount = SpContext.accumulator(0)

#Setup Broadcast variable
#SepLenAvg - we already calculated
#But make it as a broadcast variable, so that it is globally accessible across RDDs
avgSepalLen = SpContext.broadcast(SepLenAvg)

#Write a function to do the compare and count
def findHighLen(line) :
    global sepalHighCount
    attList=line.split(",")

    if is_float(attList[0]) :
        if float(attList[0]) > avgSepalLen.value :
            sepalHighCount += 1
    return
    
#map for running the count. Also do a action to force execution of map
irisRDD.map(findHighLen).count()

print(sepalHighCount)
    


