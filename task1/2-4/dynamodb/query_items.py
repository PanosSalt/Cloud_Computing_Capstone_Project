import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id='XXXXXXXXXXXXXXXXXX', aws_secret_access_key='XXXXXXXXXXXXXXXXXX')
table = dynamodb.Table('AirportsArrDelay')

###########################################################################
# Query for "CMI"
resp = table.query(KeyConditionExpression=Key('Origin').eq('"CMI"') & Key('Dest').eq(' "ORD"'))

print("The query for CMI returned the following items:")
print(resp['Items'][0]['Origin'] + " ->" + resp['Items'][0]['Dest'] + ": " + resp['Items'][0]['ArrDelay'])

###########################################################################	
# Query for "IND"
resp = table.query(KeyConditionExpression=Key('Origin').eq('"IND"') & Key('Dest').eq(' "CMH"'))

print("The query for IND returned the following items:")
print(resp['Items'][0]['Origin'] + " ->" + resp['Items'][0]['Dest'] + ": " + resp['Items'][0]['ArrDelay'])

###########################################################################
# Query for "DFW"
resp = table.query(KeyConditionExpression=Key('Origin').eq('"DFW"') & Key('Dest').eq(' "IAH"'))

print("The query for DFW returned the following items:")
print(resp['Items'][0]['Origin'] + " ->" + resp['Items'][0]['Dest'] + ": " + resp['Items'][0]['ArrDelay'])

###########################################################################
# Query for "LAX"
resp = table.query(KeyConditionExpression=Key('Origin').eq('"LAX"') & Key('Dest').eq(' "SFO"'))

print("The query for LAX returned the following items:")
print(resp['Items'][0]['Origin'] + " ->" + resp['Items'][0]['Dest'] + ": " + resp['Items'][0]['ArrDelay'])

###########################################################################
# Query for "JFK"
resp = table.query(KeyConditionExpression=Key('Origin').eq('"JFK"') & Key('Dest').eq(' "LAX"'))

print("The query for JFK returned the following items:")
print(resp['Items'][0]['Origin'] + " ->" + resp['Items'][0]['Dest'] + ": " + resp['Items'][0]['ArrDelay'])
	
###########################################################################
# Query for "ATL"
resp = table.query(KeyConditionExpression=Key('Origin').eq('"ATL"') & Key('Dest').eq(' "PHX"'))

print("The query for ATL returned the following items:")
print(resp['Items'][0]['Origin'] + " ->" + resp['Items'][0]['Dest'] + ": " + resp['Items'][0]['ArrDelay'])