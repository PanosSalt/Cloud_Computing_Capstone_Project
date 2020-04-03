#!/usr/bin/python

import sys

current_carrier = None
ave_delay = 0.0
counter = 1.0
unique_carrier = None

infile = sys.stdin
next(infile) # skip first line of input file

for line in sys.stdin:
	line = line.strip()
	unique_carrier, arr_delay = line.split('\t')
	
	try:
		arr_delay = float(arr_delay)
	except ValueError:
		# ignore lines where the arr_delay is not a number
		continue
	
	if current_carrier == unique_carrier:
		ave_delay += arr_delay
		counter = counter + 1
	else:
		ave_delay = ave_delay / counter
		if current_carrier:
			print('%s\t%s' % (current_carrier, round(ave_delay,2)))
		ave_delay = arr_delay
		current_carrier = unique_carrier
		counter = 1.0

if current_carrier == unique_carrier:
	ave_delay = ave_delay / counter
	print('%s\t%s' % (current_carrier, round(ave_delay,2)))
