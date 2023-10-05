import json
import xml.etree.ElementTree as ET
import boto3

DEFAULT_BUCKET = 'xmlresults153'
s3 = boto3.resource('s3')

def lambda_handler(event, context):

    try:
        #Get the name of the file to retrieve
        objectName = getObjectName(event)
        
        #Get the xml file from the S3 bucket
        xmlstr = getXmlString(DEFAULT_BUCKET, objectName)
        
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
    ##
    
    return {
        'statusCode': 200,
        'body': json.dumps(getClasses(xmlstr))
    }


def getObjectName(forEvent):
    race_id = forEvent['queryStringParameters']['race_id']
    return str(race_id) + '.xml'
    
def getXmlString(fromS3, named):
    s3Obj = s3.Object(fromS3, named)
    return s3Obj.get()['Body'].read()
    
def getRoor(forXml):
    return ET.fromstring(forXml)

def findClassesNames(fromRoot, usingNamespace = {'':'http://www.orienteering.org/datastandard/3.0'}):
    return fromRoot.findall('./ClassResult/Class/Name', usingNamespace)
    
def mapClassesNamesToText(fromXmlClasses):
    return [classx.text for classx in fromXmlClasses]
    
def getClasses(xml):
    root = getRoor(forXml=xml)
    class_names_elements = findClassesNames(fromRoot=root)
    return mapClassesNamesToText(fromXmlClasses=class_names_elements)
