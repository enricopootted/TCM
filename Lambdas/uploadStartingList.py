import json
import boto3

DEFAULT_BUCKET = 'xmlstartingtime153'
DYNAMO_TABLE = 'tokenGare'

def lambda_handler(event, context):
    
    race_id = str(event['queryStringParameters']['race_id'])
    race_Name = getRaceName(race_id)
    token = event['queryStringParameters']['token']

    if is_race_registered(race_id, token):
        
        xmlstring = event['body']
        put_to_bucket(xmlstring, race_Name)
        res = {'statusCode': 200, 'body': 'Starting list updloaded with key: '+race_id}
    else:
        res = {'statusCode': 453, 'body': 'Race not registered or invalid token'}
            
    return res

def put_to_bucket(bucketName=DEFAULT_BUCKET, objectName='', content=None):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucketName, objectName)
    obj.put(Body=content)

def getItems(race_id_in):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMO_TABLE)
    return table.get_item(Key={'race_id': race_id_in})
   
def is_race_registered(race_id_in, token_in):
    
    result = getItems(race_id_in)

    #Checks if an entry exists with the given name before checking the token
    if 'Item' in result:
        return {"token": result['Item']['token']} == {"token": token_in}
    else:
        return False
        
def getRaceName(race_id):
    return race_id.split('-')[0] + '.xml'