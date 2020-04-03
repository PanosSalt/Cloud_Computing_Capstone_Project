#!/usr/bin/python

import sys

#current_carrier = None
ave_delay = 0.0
counter = 1.0
#unique_carrier = None
origin_unique_carrier = None
current_origin_unique_carrier = None

for line in sys.stdin:
	line = line.strip()
	(origin_unique_carrier), dep_delay = line.split('\t')
	
	try:
		dep_delay = float(dep_delay)
	except ValueError:
		# ignore lines where the dep_delay is not a number
		continue
	
	if current_origin_unique_carrier == origin_unique_carrier:
		ave_delay += dep_delay
		counter = counter + 1
	else:
		ave_delay = ave_delay / counter
		if current_origin_unique_carrier:
			print('%s\t%s' % (current_origin_unique_carrier, round(ave_delay,2)))
		#current_origin = origin
		current_origin_unique_carrier = origin_unique_carrier
		ave_delay = dep_delay
		counter = 1.0

if (current_origin_unique_carrier == origin_unique_carrier):
	ave_delay = ave_delay / counter
	print('%s\t%s' % (current_origin_unique_carrier, round(ave_delay,2)))
