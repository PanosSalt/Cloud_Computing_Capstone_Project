import boto3
from boto3.dynamodb.conditions import Key

dynamodb = boto3.resource('dynamodb', region_name='us-east-1', aws_access_key_id='AKIA3MACAOOQ3GJTS56F', aws_secret_access_key='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')
table = dynamodb.Table('AirportsDepDelay')

###########################################################################
# Query for "CMI"
resp = table.query(KeyConditionExpression=Key('Origin').eq('"CMI"'))

print("The query for CMI returned the following items:")
sorted_items = sorted(resp['Items'], key = lambda i: float(i['DepDelay']))
for x in sorted_items[:10]:
	print(x['Dest'], x['DepDelay'])

###########################################################################	
# Query for "BWI"
resp = table.query(KeyConditionExpression=Key('Origin').eq('"BWI"'))

print("The query for BWI returned the following items:")
sorted_items = sorted(resp['Items'], key = lambda i: float(i['DepDelay']))
for x in sorted_items[:10]:
	print(x['Dest'], x['DepDelay'])

###########################################################################
# Query for "MIA"
resp = table.query(KeyConditionExpression=Key('Origin').eq('"MIA"'))

print("The query for MIA returned the following items:")
sorted_items = sorted(resp['Items'], key = lambda i: float(i['DepDelay']))
for x in sorted_items[:10]:
	print(x['Dest'], x['DepDelay'])

###########################################################################
# Query for "LAX"
resp = table.query(KeyConditionExpression=Key('Origin').eq('"LAX"'))

print("The query for LAX returned the following items:")
sorted_items = sorted(resp['Items'], key = lambda i: float(i['DepDelay']))
for x in sorted_items[:10]:
	print(x['Dest'], x['DepDelay'])

###########################################################################
# Query for "IAH"
resp = table.query(KeyConditionExpression=Key('Origin').eq('"IAH"'))

print("The query for IAH returned the following items:")
sorted_items = sorted(resp['Items'], key = lambda i: float(i['DepDelay']))
for x in sorted_items[:10]:
	print(x['Dest'], x['DepDelay'])
	
###########################################################################
# Query for "SFO"
resp = table.query(KeyConditionExpression=Key('Origin').eq('"SFO"'))

print("The query for SFO returned the following items:")
sorted_items = sorted(resp['Items'], key = lambda i: float(i['DepDelay']))
for x in sorted_items[:10]:
	print(x['Dest'], x['DepDelay'])