import xml.etree.ElementTree as ET
from operator import itemgetter
import boto3
import json

DEFAULT_BUCKET = 'xmlresults153'
s3 = boto3.resource('s3')
usingNamespace = {'':'http://www.orienteering.org/datastandard/3.0'}

def lambda_handler(event, context):
    race_name = getRaceName(event)
    club= str(event['queryStringParameters']['club'])
    
    try:
        myRoot = retrieve_file_to_json(race_name)
        classificaFinale = get_classifica_per_club(myRoot, club)

        return {
            'statusCode': 200,
            'body' : json.dumps(classificaFinale)
        }
        
    #Error Handling
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
   
def getRaceName(event):
    race_id = str(event['queryStringParameters']['race_id'])
    return race_id.split('-')[0] + '.xml'

def getObjectName(forEvent):
    raceId = forEvent['queryStringParameters']['raceId']
    return str(raceId) + '.xml'
    
def getXmlString(fromS3, named):
    s3Obj = s3.Object(fromS3, named)
    return s3Obj.get()['Body'].read()
    
def retrieve_file_to_json(race_name):
    xmlstr = getXmlString(DEFAULT_BUCKET, race_name)
    myTree = ET.ElementTree(ET.fromstring(xmlstr))
    return myTree.getroot()
    
def get_classifica_per_club(root, club):
    classificaFinale = []
    
    for child in root.findall('.//PersonResult', usingNamespace):
        atName = child.findall('.//Given', usingNamespace)
        atSurname = child.findall('.//Family', usingNamespace)
        atposition = child.findall('.//Position', usingNamespace)
        atClub=child.findall('.//Organisation/Name', usingNamespace)
        
        name = ([a.text for a in atName])
        surname = ([b.text for b in atSurname])
        position = ([c.text for c in atposition])
        retrievedClub = ([d.text for d in atClub])

        if not position:
            position.append('not arrived yet')

        if (retrievedClub[0] == club):
            rank = {}
            rank['name'] = name[0]
            rank['surname'] = surname[0]
            rank['position'] = position[0]
            classificaFinale.append(rank)
    
    return sorted(classificaFinale, key = lambda i: i['position'])