import json
import base64
import boto3
from boto3.dynamodb.conditions import Key
import requests
import time
from datetime import datetime
import os
import logging
from CEI_Logging.CEI_Logging import postLog

url = os.environ["cdfUrl"]
headers = {
    "Accept": "application/vnd.aws-cdf-v2.0+json",
    "Content-Type": "application/vnd.aws-cdf-v2.0+json",
}


def configurationHandler(event, context):
    """handles the incoming agency configuration call from the API

    Args:
        event (dictionary): configuration message body
        context (dictionary): call metadata (aws required structure, unused in function)

    Returns:
        string: results of operation
    """
    try:
        configArgs = event
        # handle for lambda proxy conversion -
        if "body" in event:
            configArgs = json.loads(event["body"])

        # url construction vars
        env = "Stage"
        siteId = configArgs["siteId"]
        agencyID = configArgs["agencyId"]
        urlAction = f"/groups/%2f{siteId}%2f{agencyID}"
        urlComplete = f"{url}/{env}/{urlAction}"

        code = requests.get(urlComplete, headers).json()

        logging.info(code)

        # masking internal 's - CDF will choke if we don't
        for item in configArgs:
            configArgs[item] = str(configArgs[item]).replace("'", "|||")

        config = {
            "attributes": {
                "CEIUnitTypeConfig": configArgs["unitTypeConfig"],
                "CEIUnitIDConfig": configArgs["unitIDConfig"],
                "CEIIncidentTypeConfig": configArgs["incidentTypeConfig"],
                "CEIIncidentStatusConfig": configArgs["incidentStatusConfig"],
                "CEIIncidentPriorityConfig": configArgs["incidentPriorityConfig"],
                "CEIUnitStatusConfig": configArgs["unitStatusConfig"],
            }
        }

        updateCall = json.dumps(config).replace("'", '"').replace("|||", "'")
        logging.info(f"Update Call = {updateCall}")
        patchResult = requests.patch(urlComplete, data=updateCall, headers=headers)
        print (patchResult.content) 
        responseJson = {
            "statusCode": 200,
            "body": f"Configuration for {configArgs['siteId']}:{configArgs['agencyId']} updated",
        }
        if (patchResult.status_code  == 404):
            responseJson = {
            "statusCode": 400 ,
            "body": f"Error updating {configArgs['siteId']}:{configArgs['agencyId']} - Agency/siteId not found",
            }
        response = {"statusCode": responseJson.get('statusCode'), "body": json.dumps(responseJson)}
        
        #logDat
        postLog(siteId,agencyID,None,None, "Info", "Configuration Message", "Configuration", json.dumps(configArgs))
        
        
        
        return response
    except KeyError as e:
        responseJson = {"statusCode": 400, "body": f"Message Format Error - {e}"}
        response = {"statusCode": 400, "body": json.dumps(responseJson)}
        return response
    except Exception as e:
        responseJson = {
            "statusCode": 500,
            "body": f"{e.__class__.__name__} Error - {e}",
        }
        response = {"statusCode": 500, "body": json.dumps(responseJson)}
        return response
