import json
import boto3
from datetime import datetime
from boto3.dynamodb.conditions import Key
import uuid

dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
def lambda_handler(event, context):

    pub_topic = event.get('topic', "")
    table = dynamodb.Table('cei-iot-pub-confirm')
    if '/SVR/EVP/2100/' in pub_topic :
        try:
            response = table.query(
            KeyConditionExpression=Key('id').eq(pub_topic)
            )
            if (response.get("Count",0)==0):
                try:
                    record = {
                      "id" : str(uuid.uuid4()),
                      "topic" : pub_topic,
                      "time" : str(datetime.utcnow())
                    }    
                    return table.put_item(Item=record)
                    
                except Exception as e:
                    response = f'Error - Unable to confirm publication {e}... '
                    record = {
                        "id" : str(uuid.uuid4()),
                        "topic" : response,
                        "time" : str(datetime.utcnow())
                    }    
                    table.put_item(Item=record)
                    return f"Fail - {e}"
                
        except Exception as e:
                    print (f"Fail - {e}")
                    return f"Fail - {e}"        
    else:
        return "fail"