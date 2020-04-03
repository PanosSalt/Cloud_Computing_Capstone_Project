#!/usr/bin/python

import sys

(current_origin_dest_flight_date, min_arr_delay) = (None, 0)
current_unique_carrier = None
current_flight_num = None
current_crs_dep_time = None

for line in sys.stdin:
	line = line.strip()
	origin_dest_flight_date, unique_carrier, flight_num, crs_dep_time, arr_delay = line.split('\t')
	
	try:
		arr_delay = float(arr_delay)
	except ValueError:
		# ignore lines where the arr_delay is not a number
		continue
	
	if current_origin_dest_flight_date and current_origin_dest_flight_date != origin_dest_flight_date:
		print('%s\t%s\t%s\t%s\t%s' % (current_origin_dest_flight_date, current_unique_carrier, current_flight_num, current_crs_dep_time, min_arr_delay))
		(current_origin_dest_flight_date, min_arr_delay) = (origin_dest_flight_date, float(arr_delay))
		current_unique_carrier = unique_carrier
		current_flight_num = flight_num
		current_crs_dep_time = crs_dep_time
	else:
		(current_origin_dest_flight_date, min_arr_delay) = (origin_dest_flight_date, min(min_arr_delay, float(arr_delay)))
		current_unique_carrier = unique_carrier
		current_flight_num = flight_num
		current_crs_dep_time = crs_dep_time
	
if current_origin_dest_flight_date:
	print('%s\t%s\t%s\t%s\t%s' % (current_origin_dest_flight_date, current_unique_carrier, current_flight_num, current_crs_dep_time, min_arr_delay))
