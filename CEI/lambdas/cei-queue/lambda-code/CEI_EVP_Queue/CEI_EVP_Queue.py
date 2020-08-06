import json
import logging
import boto3
from CEI_Logging.CEI_Logging import postLog

sqs_client = boto3.client('sqs')

def send_sqs_message(QueueName, msg_body, msg_group_id, msg_id):
    """Send the incoming incident message to the SQS 

    Args:
        QueueName (string): what queue we're talking about
        msg_body (string): body of message being processed
        msg_group_id (uuid): message group being queued (prevents double processing)
        msg_id (uuid): specific message being queued

    Returns:
        response: results of all operations
    """

    # Send the SQS message
    sqs_queue_url = sqs_client.get_queue_url(
    QueueName=QueueName
    )['QueueUrl']
    try:
        msg = sqs_client.send_message(QueueUrl=sqs_queue_url,
                                      MessageBody=json.dumps(msg_body),
                                      MessageGroupId=msg_group_id,
                                      MessageDeduplicationId=msg_id
                                      )
    except ClientError as e:
        logging.error(e)
        return None
    return msg


def queue_process(event, context):
    """[summary]

    Args:
        event ([type]): [description]
        context ([type]): [description]

    Returns:
        [type]: [description]
    """    
    
    incidentArgs = event;
    #handle for lambda proxy conversion - 
    if 'body' in event:
            incidentArgs = json.loads(event['body']);
            
    QueueName = 'cei-sqs.fifo'
    # Set up logging
    logging.basicConfig(level=logging.DEBUG,
                        format='%(levelname)s: %(asctime)s: %(message)s')
    try:
        # Send SQS message
        incidentId = incidentArgs['incidents'][0]['id']
        siteId = incidentArgs['siteId']
        agencyId = incidentArgs['agencyId']
        print(agencyId)
        eventMsg = f"Incident {incidentId} for {siteId}:{agencyId} queued for processing"
        msg = send_sqs_message(QueueName,incidentArgs, agencyId, incidentArgs['messageId'])
        if msg is not None:
            logging.info(f'Sent SQS message ID: {msg["MessageId"]}')
        responseJson = {
            'statusCode': 202,
            'body': eventMsg
        }
        response = {
            'statusCode': 202,
            'body': json.dumps(responseJson)
        }
        postLog(siteId, agencyId, "None", incidentArgs["messageId"], "Info", "Queue Operation", "CEI EVP Queue", str(json.dumps(event))) 
        return response
    except KeyError as e:
        responseJson = {
            'statusCode' : 400,
            'body' : f'Message Format Error - {e}'
        }
        response = {
            'statusCode': 400,
            'body': json.dumps(responseJson)
        }
        return response
    except Exception as e:
        responseJson = {
            'statusCode': 500,
            'body': f'{e.__class__.__name__} Error - {e}'
        }
        response = {
            'statusCode': 500,
            'body': json.dumps(responseJson)
        }
        return response
