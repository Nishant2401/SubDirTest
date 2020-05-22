import json 
import datetime
def lambda_handler(event, context):
    # TODO implement 
    print(event)
    data = {
        'output': 'Hello from '+ event['Country'],
        #'output': 'Hello from UK',
        'timestamp': datetime.datetime.utcnow().isoformat()
    }
    return {'statusCode': 200,
            'body': json.dumps(data),
            'headers': {'Content-Type': 'application/json'}}


            #Change only in Lambda1 