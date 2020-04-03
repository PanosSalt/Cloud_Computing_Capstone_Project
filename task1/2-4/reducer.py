#!/usr/bin/python

import sys

ave_delay = 0.0
counter = 1.0
origin_dest = None
current_origin_dest = None

for line in sys.stdin:
	line = line.strip()
	(origin_dest), arr_delay = line.split('\t')
	
	try:
		arr_delay = float(arr_delay)
	except ValueError:
		# ignore lines where the arr_delay is not a number
		continue
	
	if current_origin_dest == origin_dest:
		ave_delay += arr_delay
		counter = counter + 1
	else:
		ave_delay = ave_delay / counter
		if current_origin_dest:
			print('%s\t%s' % (current_origin_dest, round(ave_delay,2)))
		#current_origin = origin
		current_origin_dest = origin_dest
		ave_delay = arr_delay
		counter = 1.0

if (current_origin_dest == origin_dest):
	ave_delay = ave_delay / counter
	print('%s\t%s' % (current_origin_dest, round(ave_delay,2)))
