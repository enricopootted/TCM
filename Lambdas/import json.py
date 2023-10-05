import json
import boto3

DEFAULT_BUCKET = 'xmlresults153'
DYNAMO_TABLE = 'tokenGare'

def lambda_handler(event, context):
    
    race_Name = str(event['queryStringParameters']['raceName'])
    race_Date = str(event['queryStringParameters']['raceDate'])
    race_id = race_Name+'-'+race_Date
    token = event['queryStringParameters']['token']

    if is_race_registered(race_id_in=race_id, token_in=token):
        
        objectName = race_Name + '.xml'
        xmlstring = event['body']
        
        put_to_bucket(content=xmlstring, objectName=objectName)
        put_to_dynamo(id_gara_in=race_id, link_in=createLink(fileName=objectName))
        
        res = {'statusCode': 200, 'body': 'File updloaded with key: '+race_id}
    else:
        res = {'statusCode': 453, 'body': 'Race not registered or invalid token'}
            
    return res

def put_to_bucket(bucketName=DEFAULT_BUCKET, objectName='', content=None):
    s3 = boto3.resource('s3')
    obj = s3.Object(bucketName, objectName)
    obj.put(Body=content)

def createLink(bucketName=DEFAULT_BUCKET, fileName=''):
    return 'https://' + bucketName + '.s3.amazonaws.com/' + fileName

def put_to_dynamo(id_gara_in="", link_in=""):
   dynamodb = boto3.client('dynamodb')
   dynamodb.put_item(TableName='urlGareSalvate', Item={'id_gara':{'S':id_gara_in}, 'link':{'S':link_in}})
   
def getItems(race_id_in=''):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMO_TABLE)
    return table.get_item(Key={'race_id': race_id_in})
   
def is_race_registered(race_id_in='', token_in=''):
    
    result = getItems(race_id_in=race_id_in)

    #Checks if an entry exists with the given name before checking the token
    if 'Item' in result:
        return {"token": result['Item']['token']} == {"token": token_in}
    else:
        return False