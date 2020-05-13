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

# import boto3
# import json

# ec2 = boto3.client('ec2')
# def lambda_handler(event, context):
#     print(event)
#     response = ec2.describe_availability_zones()
#     return {"statusCode": 200, "body": json.dumps(response)}