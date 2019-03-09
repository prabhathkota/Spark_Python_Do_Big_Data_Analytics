# -*- coding: utf-8 -*-

#............................................................................
##   Loading and Storing Data
#............................................................................

#Initialize SparkSession and SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext

#Create a Spark Session
SpSession = SparkSession \
    .builder \
    .master("local[2]") \ #[2] is no of partitions
    .appName("jlyang_spark") \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max","2") \
    .config("spark.sql.warehouse.dir", "/Users/jlyang/Spark/spark-warehouse")\
    .getOrCreate()

#Get the Spark Context from Spark Session
SpContext = SpSession.sparkContext

#Load from a collection
collData = SpContext.parallelize([4,3,8,5,8])
collData.collect()  # bring the entire RDD to the driver node, could be expensive


#Load the file. Lazy initialization
autoData = SpContext.textFile("auto-data.csv")
autoData.cache()
#Loads only now.
print autoData.count() #198
print autoData.first() #prints 1st line
print autoData.take(5) #gives you a list of 5 elements

#This will print all lines
#for line in autoData.collect():
#    print(line)
    
#Save to a local file. First collect the RDD to the master
#and then save as local file.
autoDataFile = open("auto-data-saved.csv","w")
autoDataFile.write("\n".join(autoData.collect()))
autoDataFile.close()

#............................................................................
##   Transformations
#............................................................................

#Map and create a new RDD
tsvData=autoData.map(lambda x : x.replace(",","\t"))
print tsvData.take(5)

#Filter and create a new RDD
toyotaData=autoData.filter(lambda x: "toyota" in x)
print toyotaData #RDD object it prints
print toyotaData.count() #32

#FlatMap
words=toyotaData.flatMap(lambda line: line.split(","))
print words.count() #384 words in 32 lines of toyota
print words.take(20) #it takes 20 words from 384 and print
print '----'    

#Distinct
for numbData in collData.distinct().collect():
    print(numbData)
print '----'    

#Set operations
words1 = SpContext.parallelize(["hello","war","peace","world"])
words2 = SpContext.parallelize(["war","peace","universe"])

for unions in words1.union(words2).distinct().collect():
    print(unions)

print '----'    
for intersects in words1.intersection(words2).collect():
    print(intersects)
    
print '----'    
#Using functions for transformation
#cleanse and transform an RDD
def cleanseRDD(autoStr) :
    if isinstance(autoStr, int) :
        return autoStr
    attList=autoStr.split(",")
    #convert doors to a number str
    if attList[3] == "two" :
         attList[3]="2"
    else :
         attList[3]="4"
    #Convert Drive to uppercase
    attList[5] = attList[5].upper()
    return ",".join(attList)
    
cleanedData=autoData.map(cleanseRDD)
print cleanedData.collect() #it returns a List

#............................................................................
##   Actions
#............................................................................

#reduce - compute the sum
collData.collect()  #[4,3,8,5,8]
print collData.reduce(lambda x,y: x+y) #28

print '----'    
#find the shortest line - reduce() RDD function 
print autoData.reduce(lambda x,y: x if len(x) < len(y) else y)

print '----'    

#Use a function to perform reduce 
def getMPG( autoStr) :
    if isinstance(autoStr, int) :
        return autoStr
    attList=autoStr.split(",")
    if attList[9].isdigit() :
        return int(attList[9])
    else:
        return 0

#find average MPG-City for all cars    
print autoData.reduce(lambda x,y : getMPG(x) + getMPG(y)) \
    / (autoData.count()-1.0)  # account for header line
    
print '----'    
#............................................................................
##   Working with Key/Value RDDs
#............................................................................

#create a Key Value RDD of auto Brand and Horsepower
cylData = autoData.map( lambda x: ( x.split(",")[0], \
    x.split(",")[7]))
print cylData.count() #198
print '----'    
print cylData.take(5) #take first 5 key-value pairs
print '----'    
print cylData.keys().collect() #get all keys from key-value pair RDD cylData
print '----'    

#Remove header row
header = cylData.first()
print header
cylHPData= cylData.filter(lambda line: line != header) #get data without header
print cylHPData.count() #197

print '----'    

#Find average HP by Brand
#Add a count 1 to each record and then reduce to find totals of HP and counts
addOne = cylHPData.mapValues(lambda x: (x, 1))
print addOne.collect() #(u'bmw', (u'182', 1)), (u'mercedes-benz', (u'123', 1))......, (u'bmw', (u'182', 1)), (u'mercedes-benz', (u'184', 1)) ]

print '----'    
brandValues= addOne \
    .reduceByKey(lambda x, y: (int(x[0]) + int(y[0]), x[1] + y[1])) 
print brandValues.collect() #(u'mercedes-benz', (1170, 8)), (u'mitsubishi', (1353, 13)), (u'saab', (760, 6)), (u'volkswagen', (973, 12))
print '----'    

#find average by dividing HP total by count total
print brandValues.mapValues(lambda x: int(x[0])/int(x[1])). \
    collect()
#[(u'dodge', 84), (u'mercury', 175), (u'jaguar', 204), (u'alfa-romero', 125), (u'nissan', 102), (u'toyota', 92), (u'plymouth', 86), (u'mazda', 86), (u'subaru', 86), (u'peugot', 99), (u'porsche', 191), (u'isuzu', 84), (u'chevrolet', 62), (u'honda', 80), (u'volvo', 128), (u'bmw', 138), (u'mercedes-benz', 146), (u'mitsubishi', 104), (u'saab', 126), (u'volkswagen', 81), (u'audi', 114)]

print '----'    

#............................................................................
##   Advanced Spark : Accumulators & Broadcast Variables
#............................................................................

#function that splits the line as well as counts sedans and hatchbacks
#Speed optimization

    
#Initialize accumulator
sedanCount = SpContext.accumulator(0)
hatchbackCount = SpContext.accumulator(0)

#Set Broadcast variable
sedanText=SpContext.broadcast("sedan")
hatchbackText=SpContext.broadcast("hatchback")

def splitLines(line) :
    global sedanCount
    global hatchbackCount

    #Use broadcast variable to do comparison and set accumulator
    if sedanText.value in line:
       sedanCount += 1
    if hatchbackText.value in line:
       hatchbackCount += 1
        
    return line.split(",")


#do the map
splitData=autoData.map(splitLines)

#Make it execute the map (lazy execution)
print splitData.count()
print '----'    
print(sedanCount, hatchbackCount)
print '----'    

#............................................................................
##   Advanced Spark : Partitions
#............................................................................
print collData.getNumPartitions()

#Specify no. of partitions.
collData = SpContext.parallelize([3,5,4,7,4], 4)
print collData.cache()
print collData.count()

print '----'    
print collData.getNumPartitions()

print '----'    
#localhost:4040 shows the current spark instance
