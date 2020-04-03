#!/usr/bin/python

import boto3
import subprocess

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id='AKIA3MACAOOQ3GJTS56F', aws_secret_access_key='w9OXeeQcPnL2FAFSB5BRw4Ww8NiRsiubnCyRbkxw')

try:
    resp = dynamodb.create_table(
        TableName="Flights",
        # Declare your Primary Key in the KeySchema argument
        KeySchema=[
            {
                "AttributeName": "Origin_Dest",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "FlightDate_AM_PM",
                "KeyType": "RANGE"
            }
        ],
        # Any attributes used in KeySchema or Indexes must be declared in AttributeDefinitions
        AttributeDefinitions=[
            {
                "AttributeName": "Origin_Dest",
                "AttributeType": "S"
            },
            {
                "AttributeName": "FlightDate_AM_PM",
                "AttributeType": "S"
            }
        ],
        # ProvisionedThroughput controls the amount of data you can read or write to DynamoDB per second.
        # You can control read and write capacity independently.
        ProvisionedThroughput={
            "ReadCapacityUnits": 50,
            "WriteCapacityUnits": 50
        }
    )
    print("Table created successfully!")
except Exception as e:
    print("Error creating table:")
    print(e)

table = dynamodb.Table('Flights')
cat = subprocess.Popen(["hdfs", "dfs", "-cat", "/task1-group3-2-hadoop/part-00000"], stdout=subprocess.PIPE)

with table.batch_writer() as batch:
	for line in cat.stdout:
		line = line.strip()
		flight, unique_carrier, flight_num, crs_dep_time, dep_delay = line.decode().split('\t')
		flight = flight.replace("('","")
		flight = flight.replace("')","")
		flight = flight.replace("'","")
		flight = flight.replace("\"","")
		origin, dest, am_pm, flight_date = flight.split(',')
		if (origin + ' ->' + dest == 'CMI -> ORD') or (origin + ' ->' + dest == 'ORD -> LAX') \
				or (origin + ' ->' + dest == 'JAX -> DFW') or (origin + ' ->' + dest == 'DFW -> CRP') \
				or (origin + ' ->' + dest == 'SLC -> BFL') or (origin + ' ->' + dest == 'BFL -> LAX') \
				or (origin + ' ->' + dest == 'LAX -> SFO') or (origin + ' ->' + dest == 'SFO -> PHX') \
				or (origin + ' ->' + dest == 'DFW -> ORD') or (origin + ' ->' + dest == 'ORD -> DFW') \
				or (origin + ' ->' + dest == 'LAX -> ORD') or (origin + ' ->' + dest == 'ORD -> JFK'):
			print(origin + ' ->' + dest, flight_date + am_pm, unique_carrier, flight_num, crs_dep_time, dep_delay)
			batch.put_item(Item={"Origin_Dest": origin + ' ->' + dest, "FlightDate_AM_PM": flight_date + am_pm, "UniqueCarrier": unique_carrier, "FlightNum": flight_num, "CRSDepTime": crs_dep_time, "DepDelay": dep_delay})


