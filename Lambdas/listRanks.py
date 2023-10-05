import xml.etree.ElementTree as ET
from operator import itemgetter
import boto3
import json

DEFAULT_BUCKET = 'xmlresults153'
s3 = boto3.resource('s3')
usingNamespace = {'':'http://www.orienteering.org/datastandard/3.0'}

def lambda_handler(event, context):
    race_name = getRaceName(event)
    
    try:
        results = getResults(race_name)
        
        clubs = []
        for child in results.findall('.//PersonResult', usingNamespace):
            atClub=child.findall('.//Organisation/Name', usingNamespace)
            club = ([d.text for d in atClub])
            
            clubs.append(club[0])
            
        return {
            'statusCode' : 200,
            'body' : json.dumps(removeDuplicates(clubs))
        }
        
    ##Error Handling
    except s3.meta.client.exceptions.NoSuchKey:
        return {
            'statusCode': 404,
            'body': json.dumps("ERROR: Race ID not found.")
        }
    except KeyError:
        return {
            'statusCode': 400,
            'body': json.dumps("ERROR: 'raceId' parameter not specified.")
        }

def getXmlString(fromS3, named):
    s3Obj = s3.Object(fromS3, named)
    return s3Obj.get()['Body'].read()

def getRaceName(event):
    race_id = str(event['queryStringParameters']['race_id'])
    return race_id.split('-')[0] + '.xml'
    
def getResults(race_name):
    xmlstr = getXmlString(DEFAULT_BUCKET, race_name)
    myTree = ET.ElementTree(ET.fromstring(xmlstr))
    return myTree.getroot()
    
def removeDuplicates(fromList):
    return list(dict.fromkeys(fromList))
