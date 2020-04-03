#!/usr/bin/python

import boto3
import subprocess

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id='AKIA3MACAOOQ3GJTS56F', aws_secret_access_key='w9OXeeQcPnL2FAFSB5BRw4Ww8NiRsiubnCyRbkxw')

try:
    resp = dynamodb.create_table(
        TableName="AirportsDepDelay",
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

table = dynamodb.Table('AirportsDepDelay')
cat = subprocess.Popen(["hdfs", "dfs", "-cat", "/task1-group2-2-hadoop/part-00000"], stdout=subprocess.PIPE)

with table.batch_writer() as batch:
	for line in cat.stdout:
		line = line.strip()
		origin_dest, dep_delay = line.decode().split('\t')
		origin_dest = origin_dest.replace("('","")
		origin_dest = origin_dest.replace("')","")
		origin_dest = origin_dest.replace("'","")
		origin, dest = origin_dest.split(',')
		if origin == '"CMI"' or origin == '"BWI"' or origin == '"MIA"' or origin == '"LAX"' or origin == '"IAH"' or origin == '"SFO"': 
			print(origin, dest, dep_delay)
			batch.put_item(Item={"Origin": origin, "Dest": dest, "DepDelay": dep_delay})
