#!/bin/bash
DATA_FOLDER=$1
TARGET_FOLDER=$2

if [ ! $TARGET_FOLDER ]; then
	echo "TARGET_FOLDER unset"
	exit -1
fi

if [ ! -d $DATA_FOLDER ]; then
	echo "${DATA_FOLDER} not found"
	exit -1
fi

for FILE in `ls $DATA_FOLDER/airline_ontime/*/*.zip`; do
	for CSV_NAME in `unzip -l $FILE  | grep csv | tr -s ' ' | cut -d ' ' -f4`; do
		unzip -p $FILE $CSV_NAME | sed "s/, / /g" | cut -d ',' -f 1,2,3,4,5,6,7,11,12,18,24,25,26,27,35,36,37,38,42,44 > $TARGET_FOLDER/$CSV_NAME
		echo "$CSV_NAME from $FILE put in $TARGET_FOLDER"
	done 
done