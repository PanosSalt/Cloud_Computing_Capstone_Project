#!/usr/bin/python

import boto3
import subprocess

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id='XXXXXXXXXXXXXXXXXX', aws_secret_access_key='XXXXXXXXXXXXXXXXXX')

try:
    resp = dynamodb.create_table(
        TableName="CarrierDepDelay",
        # Declare your Primary Key in the KeySchema argument
        KeySchema=[
            {
                "AttributeName": "Origin",
                "KeyType": "HASH"
            },
            {
                "AttributeName": "UniqueCarrier",
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
                "AttributeName": "UniqueCarrier",
                "AttributeType": "S"
            }
        ],
        # ProvisionedThroughput controls the amount of data you can read or write to DynamoDB per second.
        # You can control read and write capacity independently.
        ProvisionedThroughput={
            "ReadCapacityUnits": 5,
            "WriteCapacityUnits": 5
        }
    )
    print("Table created successfully!")
except Exception as e:
    print("Error creating table:")
    print(e)

table = dynamodb.Table('CarrierDepDelay')
cat = subprocess.Popen(["hdfs", "dfs", "-cat", "/task1-group2-1-hadoop/part-00000"], stdout=subprocess.PIPE)

with table.batch_writer() as batch:
	for line in cat.stdout:
		line = line.strip()
		origin_unique_carrier, dep_delay = line.decode().split('\t')
		origin_unique_carrier = origin_unique_carrier.replace("('","")
		origin_unique_carrier = origin_unique_carrier.replace("')","")
		origin_unique_carrier = origin_unique_carrier.replace("'","")
		origin, unique_carrier = origin_unique_carrier.split(',')
		print(origin, unique_carrier, dep_delay)
		batch.put_item(Item={"Origin": origin, "UniqueCarrier": unique_carrier, "DepDelay": dep_delay})
