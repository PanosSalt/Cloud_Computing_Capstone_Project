import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id='XXXXXXXXXXXXXXXXXX', aws_secret_access_key='XXXXXXXXXXXXXXXXXX')
table = dynamodb.Table('Flights')

###########################################################################
# Query for CMI → ORD → LAX, 04/03/2008
print("Tom wants to fly from \"CMI\" to \"ORD\" at 2008-03-04 and from \"ORD\" to \"LAX\" at 2008-03-06 and will take the following flights:")
resp1 = table.query(KeyConditionExpression=Key('Origin_Dest').eq('CMI -> ORD') & Key('FlightDate_AM_PM').eq(' 2008-03-04 AM'))
for item in resp1['Items']:
	print(item)

resp2 = table.query(KeyConditionExpression=Key('Origin_Dest').eq('ORD -> LAX') & Key('FlightDate_AM_PM').eq(' 2008-03-06 PM'))
for item in resp2['Items']:
	print(item)
	
print("Total arrival delay: %s" % (float(resp1['Items'][0]['DepDelay']) + float(resp2['Items'][0]['DepDelay'])))
print("\n")
###########################################################################
# Query for JAX → DFW → CRP, 09/09/2008
print("Tom wants to fly from \"JAX\" to \"DFW\" at 2008-09-09 and from \"DFW\" to \"CRP\" at 2008-09-11 and will take the following flights:")
resp1 = table.query(KeyConditionExpression=Key('Origin_Dest').eq('JAX -> DFW') & Key('FlightDate_AM_PM').eq(' 2008-09-09 AM'))
for item in resp1['Items']:
	print(item)

resp2 = table.query(KeyConditionExpression=Key('Origin_Dest').eq('DFW -> CRP') & Key('FlightDate_AM_PM').eq(' 2008-09-11 PM'))
for item in resp2['Items']:
	print(item)
	
print("Total arrival delay: %s" % (float(resp1['Items'][0]['DepDelay']) + float(resp2['Items'][0]['DepDelay'])))
print("\n")
"""
###########################################################################
# Query for SLC → BFL → LAX, 01/04/2008
print("Tom wants to fly from \"SLC\" to \"BFL\" at 2008-01-04 and from \"BFL\" to \"LAX\" at 2008-03-04 and will take the following flights:")
resp1 = table.query(KeyConditionExpression=Key('Origin_Dest').eq('SLC -> BFL') & Key('FlightDate_AM_PM').eq(' 2008-01-04 AM'))
for item in resp1['Items']:
	print(item)

resp2 = table.query(KeyConditionExpression=Key('Origin_Dest').eq('BFL -> LAX') & Key('FlightDate_AM_PM').eq(' 2008-03-04 PM'))
for item in resp2['Items']:
	print(item)
	
print("Total arrival delay: %s" % (float(resp1['Items'][0]['DepDelay']) + float(resp2['Items'][0]['DepDelay'])))
print("\n")
"""
###########################################################################
# Query for LAX → SFO → PHX, 12/07/2008
print("Tom wants to fly from \"LAX\" to \"SFO\" at 2008-07-12 and from \"SFO\" to \"PHX\" at 2008-07-14 and will take the following flights:")
resp1 = table.query(KeyConditionExpression=Key('Origin_Dest').eq('LAX -> SFO') & Key('FlightDate_AM_PM').eq(' 2008-07-12 AM'))
for item in resp1['Items']:
	print(item)

resp2 = table.query(KeyConditionExpression=Key('Origin_Dest').eq('SFO -> PHX') & Key('FlightDate_AM_PM').eq(' 2008-07-14 PM'))
for item in resp2['Items']:
	print(item)
	
print("Total arrival delay: %s" % (float(resp1['Items'][0]['DepDelay']) + float(resp2['Items'][0]['DepDelay'])))
print("\n")
###########################################################################
# Query for DFW → ORD → DFW, 10/06/2008
print("Tom wants to fly from \"DFW\" to \"ORD\" at 2008-06-10 and from \"ORD\" to \"DFW\" at 2008-06-12 and will take the following flights:")
resp1 = table.query(KeyConditionExpression=Key('Origin_Dest').eq('DFW -> ORD') & Key('FlightDate_AM_PM').eq(' 2008-06-10 AM'))
for item in resp1['Items']:
	print(item)

resp2 = table.query(KeyConditionExpression=Key('Origin_Dest').eq('ORD -> DFW') & Key('FlightDate_AM_PM').eq(' 2008-06-12 PM'))
for item in resp2['Items']:
	print(item)
	
print("Total arrival delay: %s" % (float(resp1['Items'][0]['DepDelay']) + float(resp2['Items'][0]['DepDelay'])))
print("\n")
###########################################################################
# Query for LAX → ORD → JFK: 01/01/2008
print("Tom wants to fly from \"LAX\" to \"ORD\" at 2008-01-01 and from \"ORD\" to \"JFK\" at 2008-01-03 and will take the following flights:")
resp1 = table.query(KeyConditionExpression=Key('Origin_Dest').eq('LAX -> ORD') & Key('FlightDate_AM_PM').eq(' 2008-01-01 AM'))
for item in resp1['Items']:
	print(item)

resp2 = table.query(KeyConditionExpression=Key('Origin_Dest').eq('ORD -> JFK') & Key('FlightDate_AM_PM').eq(' 2008-01-03 PM'))
for item in resp2['Items']:
	print(item)
	
print("Total arrival delay: %s" % (float(resp1['Items'][0]['DepDelay']) + float(resp2['Items'][0]['DepDelay'])))
print("\n")