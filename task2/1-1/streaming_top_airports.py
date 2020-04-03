#!/usr/bin/python
"""
$SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8-assembly_2.11:2.4.3 --jars external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar /home/ubuntu/task2/1-1/streaming_top_airports.py [kafka_private_ip]:9092 /home/ubuntu/task2/1-1/topten_airports.log
"""
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from operator import itemgetter
import sys
import time

def printResults(rdd):
    print "----------------- TOP 10 ----------------------"
    for line in rdd.take(10):
        print line
    print "SIZE: %d" % rdd.count()

def saveResults(rdd):
    topten = rdd.take(10)
    if len(topten) == 0:
		return
    file = open(sys.argv[2], 'w')
    file.writelines(["%s  %s\n" % (item[1], item[0])  for item in topten])

def cutOffTopTen(iterable):
	topTen = []
	for tupl in iterable:
		if len(topTen) < 10:
			topTen.append(tupl)
			topTen.sort(reverse=True)
		elif topTen[9][0] < tupl[0]:
			topTen[9] = tupl
			topTen.sort(reverse=True)
	return iter(topTen)

def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    # add the new values with the previous running count to get the new count
    return sum(newValues, runningCount)

sc = SparkContext(appName="TopTenAirports")
sc.setLogLevel('ERROR')

# Create a local StreamingContext
ssc = StreamingContext(sc, 1)
ssc.checkpoint("/checkpoints/1-1/")
lines = KafkaUtils.createDirectStream(ssc, ['test1'], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Split each line by separator
lines = lines.map(lambda tup: tup[1])
rows = lines.map(lambda line: line.split(','))

# Get the airports
airports = rows.flatMap(lambda row: [row[8], row[9]])

# Count them
airportsCounted = airports.map(lambda airport: (airport, 1)).updateStateByKey(updateFunction)

# Filter top ten
sorted = airportsCounted.map(lambda tuple: (tuple[1], tuple[0]))

# We filter at each worker by partition as well, reducing shuffling time between each workers
sorted = sorted.transform(lambda rdd: rdd.mapPartitions(cutOffTopTen))

# Final sorting
sorted = sorted.transform(lambda rdd: rdd.sortByKey(False))

# Saving and debugging
sorted.foreachRDD(lambda rdd: printResults(rdd))
sorted.foreachRDD(lambda rdd: saveResults(rdd))


ssc.start()             # Start the computation
ssc.awaitTermination()
if KeyboardInterrupt:
    print("Starting Spark...")
    ssc.stop(stopSparkContext=True, stopGraceFully=True)