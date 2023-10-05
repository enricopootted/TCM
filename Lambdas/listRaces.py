import json
import boto3

DYNAMO_TABLE = 'tokenGare'

def lambda_handler(event, context):
    
    response = get_all_items()
    
    return {
        'statusCode': 200,
        'body': json.dumps(response)
    }

def get_all_items():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(DYNAMO_TABLE)
    
    all_items = table.scan()
    race_items = all_items['Items']

    return remove_token_and_mail(from_array=race_items)

def remove_token_and_mail(from_array):
    for item in from_array:
        item.pop('token')
        item.pop('mail')
        
    return from_array
