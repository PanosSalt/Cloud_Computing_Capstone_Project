#!/usr/bin/python
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import SimpleProducer, KafkaClient
import sys

def exception_handler(exception_type, exception, traceback):
    print "%s: %s" % (exception_type.__name__, exception)

def printResults(rdd):
    """
    Print results to screen.
    """
    print "----------------- SNAPSHOT ----------------------"
    for line in rdd.collect():
        print line
    print "SIZE: %d" % rdd.count()


def saveResults(rdd):
	"""
	Save results as a report.
	"""
	results = rdd.collect();
	if len(results) == 0:
		return
	file = open(sys.argv[2], 'w')
	for item in results:
		file.write("%s -> %s on %s: Flight: %s %s at %s. Arrival Delay: %s\n" % \
			(item[0][0], item[0][1], item[0][2], item[1][0], item[1][1], item[1][2], item[1][3]))


def AMOrPM(departureTime):
	depTimeHours = departureTime[1:3]
	try:
		depTimeHours = int(depTimeHours)
	except ValueError:
		pass
	if depTimeHours < 12:
		return 'AM'
	else:
		return 'PM'

def departureTimePretty(departureTime):
	depTimeHours = departureTime[1:3]
	depTimeMinutes = departureTime[3:5]
	return '%s:%s' % (depTimeHours, depTimeMinutes)


def getMinimum(newValues, currentMin):
	if currentMin is None:
	    currentMin = newValues[0]
	# Get minimum from all
	newValues.append(currentMin)
	newMin = min(newValues, key=lambda item: item[3])
	return newMin

# MAIN

sc = SparkContext(appName="BestFlights")
sc.setLogLevel('OFF')

# Create a local StreamingContext
ssc = StreamingContext(sc, 1)
ssc.checkpoint("/checkpoints/checkpoint-best-flights/")
lines = KafkaUtils.createDirectStream(ssc, ['input_2008'], {"metadata.broker.list": sys.argv[1], "auto.offset.reset":"smallest"})

# Filter only for data in 2008 and Split each line by separator
lines = lines.map(lambda tup: str(tup[1]))
rows = lines.map(lambda line: line.split(','))

# Get relevant data
rows = rows.filter(lambda row: row[16] != '')
airports_fromto = rows.map(lambda row: ((row[8], row[9], row[5], AMOrPM(row[10])), (row[6], row[7], departureTimePretty(row[10]), float(row[16]))))

# Filtering just necessary flights
airports_fromto = airports_fromto.filter(lambda row: row[0] == ('"BOS"', '"ATL"', '2008-04-03', 'AM')) \
		.union(airports_fromto.filter(lambda row: row[0] == ('"ATL"', '"LAX"', '2008-04-05', 'PM'))) \
		.union(airports_fromto.filter(lambda row: row[0] == ('"PHX"', '"JFK"', '2008-09-07', 'AM'))) \
		.union(airports_fromto.filter(lambda row: row[0] == ('"JFK"', '"MSP"', '2008-09-09', 'PM'))) \
		.union(airports_fromto.filter(lambda row: row[0] == ('"DFW"', '"STL"', '2008-01-24', 'AM'))) \
		.union(airports_fromto.filter(lambda row: row[0] == ('"STL"', '"ORD"', '2008-01-26', 'PM'))) \
		.union(airports_fromto.filter(lambda row: row[0] == ('"LAX"', '"MIA"', '2008-05-16', 'AM'))) \
		.union(airports_fromto.filter(lambda row: row[0] == ('"MIA"', '"LAX"', '2008-05-18', 'PM')))


# Minimum search
airports_fromto = airports_fromto.updateStateByKey(getMinimum)

# Print and save
airports_fromto.foreachRDD(printResults)
airports_fromto.foreachRDD(saveResults)

if __name__ == "__main__":
    sys.excepthook = exception_handler
    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
    if KeyboardInterrupt:
        print("Shutting down Spark...")
        ssc.stop(stopSparkContext=True, stopGraceFully=True)