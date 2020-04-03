#!/usr/bin/python

import sys

for line in sys.stdin:
	line = line.strip()
	line = line.replace("\"","")
	columns = line.split(',') # split line into parts
	try:
		columns[10] = int(columns[10])
	except ValueError:
		# ignore lines where the arr_delay is not a number
		continue

	if columns[16] != '':
		origin = columns[8]
		dest = columns[9]
		unique_carrier = columns[6]
		flight_num = columns[7]
		crs_dep_time = columns[10]
		flight_date = columns[5]
		arr_delay = columns[16]
		if int(crs_dep_time) < 1200:
			am_pm = "AM"
		else:
			am_pm = "PM"
		print('%s\t%s\t%s\t%s\t%s' % ((origin, dest, am_pm, flight_date), unique_carrier, flight_num, crs_dep_time, arr_delay))