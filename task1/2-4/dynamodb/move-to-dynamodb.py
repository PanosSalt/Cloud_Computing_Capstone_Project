#!/usr/bin/python

import boto3
import subprocess

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id='XXXXXXXXXXXXXXXXXX', aws_secret_access_key='XXXXXXXXXXXXXXXXXX')

try:
    resp = dynamodb.create_table(
        TableName="AirportsArrDelay",
        # Declare your Primary Key in the KeySchema argument
        KeySchema=[
            {
                "AttributeName": "Origin",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "Dest",
                "KeyType": "RANGE"
            }
        ],
        # Any attributes used in KeySchema or Indexes must be declared in AttributeDefinitions
        AttributeDefinitions=[
            {
                "AttributeName": "Origin",
                "AttributeType": "S"
            },
            {
                "AttributeName": "Dest",
                "AttributeType": "S"
            }
        ],
        # ProvisionedThroughput controls the amount of data you can read or write to DynamoDB per second.
        # You can control read and write capacity independently.
        ProvisionedThroughput={
            "ReadCapacityUnits": 1,
            "WriteCapacityUnits": 1
        }
    )
    print("Table created successfully!")
except Exception as e:
    print("Error creating table:")
    print(e)

table = dynamodb.Table('AirportsArrDelay')
cat = subprocess.Popen(["hdfs", "dfs", "-cat", "/task1-group12-4-hadoop/part-00000"], stdout=subprocess.PIPE)

with table.batch_writer() as batch:
	for line in cat.stdout:
		line = line.strip()
		origin_dest, arr_delay = line.decode().split('\t')
		origin_dest = origin_dest.replace("('","")
		origin_dest = origin_dest.replace("')","")
		origin_dest = origin_dest.replace("'","")
		origin, dest = origin_dest.split(',')
		if origin == '"CMI"' or origin == '"IND"' or origin == '"DFW"' or origin == '"LAX"' or origin == '"JFK"' or origin == '"ATL"': 
			print(origin, dest, arr_delay)
			batch.put_item(Item={"Origin": origin, "Dest": dest, "ArrDelay": arr_delay})
