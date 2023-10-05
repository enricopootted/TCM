import xml.etree.ElementTree as ET
from operator import itemgetter
import boto3
import json
import datetime

DEFAULT_BUCKET = 'xmlstartingtime153'
s3 = boto3.resource('s3')
usingNamespace = {'':'http://www.orienteering.org/datastandard/3.0'}

def lambda_handler(event, context):
    race_name = getRaceName(event)

    try:
        myRoot = retrieve_file_to_json(race_name)
        classificaFinaleOrdinata = get_starting_time(myRoot)
        
        return {
            'statusCode': 200,
            'body' : json.dumps(classificaFinaleOrdinata)
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
   


def getRaceName(event):
    race_id = str(event['queryStringParameters']['race_id'])
    return race_id.split('-')[0] + '.xml'
    
def getXmlString(fromS3, named):
    s3Obj = s3.Object(fromS3, named)
    return s3Obj.get()['Body'].read()
    
def retrieve_file_to_json(race_name):
    xmlstr = getXmlString(DEFAULT_BUCKET, race_name)
    myTree = ET.ElementTree(ET.fromstring(xmlstr))
    return myTree.getroot()

def get_starting_time(myRoot):
    classificaFinale = []
    
    for child in myRoot.findall('.//ClassStart', usingNamespace):
        atcategory=child.findall('.//Class/Name', usingNamespace)
        cat= ([a.text for a in atcategory])
        for subchild in child.findall('.//PersonStart', usingNamespace):
            
        
            atTime=subchild.findall('.//StartTime',usingNamespace)
            atName = subchild.findall('.//Given', usingNamespace)
            atSurname = subchild.findall('.//Family', usingNamespace)
            startTime = ([c.text for c in atTime])
            
            name = ([a.text for a in atName])
            surname = ([b.text for b in atSurname])
            temposplit=startTime[0].split('+')
            temposplit2=temposplit[0].split('T')
            tempoPersonale=datetime.datetime.strptime(temposplit2[1], '%H:%M:%S')
            timee = tempoPersonale.strftime("%H:%M:%S")
            
            starting = {}
            starting['startTime'] = timee
            starting['name'] = name[0]
            starting['surname'] = surname[0]
            
            classificaFinale.append(starting)
            
    return sorted(classificaFinale, key = lambda i: i['startTime'])
