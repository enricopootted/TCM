import xml.etree.ElementTree as ET
from operator import itemgetter
import boto3
import json

DEFAULT_BUCKET = 'xmlresults153'
s3 = boto3.resource('s3')
usingNamespace = {'':'http://www.orienteering.org/datastandard/3.0'}

def lambda_handler(event, context):
    
    race_name = getRaceName(event)
    category = str(event['queryStringParameters']['className'])

    try:
        myRoot = retrieve_file_to_json(race_name)
        classifica_finale = get_classifica(myRoot, category)

        return {
            'statusCode' : 200,
            'body': json.dumps(classifica_finale)
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
    
def get_classifica(myRoot, category):
    classificaFinale = []

    for athlete in myRoot.findall('.//ClassResult', usingNamespace):
        atcategory = athlete.findall('.//Class/Name', usingNamespace)
        retrievedCategory = ([d.text for d in atcategory])
        for child in athlete.findall('.//PersonResult', usingNamespace):
            atName = child.findall('.//Given', usingNamespace)
            atSurname = child.findall('.//Family', usingNamespace)
            atposition = child.findall('.//Position', usingNamespace)
            
            name = ([a.text for a in atName])
            surname = ([b.text for b in atSurname])
            position = ([c.text for c in atposition])
            
            #se non è stata ancora inserita la posizione(atlete non arrivato), viene inserita stringa 'not arrived'
            if not position:
                position.append('Not arrived')
                
            if not(retrievedCategory[0] == category):
                break
            
            rank = {}
            rank['position'] = position[0]
            rank['name'] = name[0]
            rank['surname'] = surname[0]
            rank['category'] = retrievedCategory[0]
            
            classificaFinale.append(rank)
    
    return sorted(classificaFinale, key = lambda i: i['position'])
