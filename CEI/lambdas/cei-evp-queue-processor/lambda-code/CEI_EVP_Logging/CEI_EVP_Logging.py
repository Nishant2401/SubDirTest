import json
import uuid
import time
import boto3
import logging
import os
import requests

url = os.environ["cdfUrl"]
client = boto3.client("firehose", region_name="us-west-2")
headers = {
    "Accept": "application/vnd.aws-cdf-v2.0+json",
    "Content-Type": "application/vnd.aws-cdf-v2.0+json",
}


def postLog(
    siteId, agencyId, deviceCEIID, CADmessageId, category, subtype, source, message,
):
    """log action - push log to S3 Via Kinesis Firehose

    Args:
        siteId (uuid): Id of site/installation source of message
        siteName (string): name of site/installation source of message
        agencyId (uuid): id of agency source of message
        deviceCEIID (uuid): deviceId of the device in question
        CADmessageId (uuid): id of message from CAD
        category (string): broad type of the log being passed
        subtype (string): specific type of the log beind passed
        source (string): process/code source of the message 
        message (string): log details being recorded. 
    """
    try:
        LogData = []
        log = {
            "logid": str(uuid.uuid4()),
            "timestamp": str(time.time()),
            "siteid": str(siteId),
            "agencyid": str(agencyId),
            "agencyname": getAgencyName(siteId, agencyId),
            "deviceid": getDeviceId(deviceCEIID, siteId, agencyId),
            "deviceceiid": str(deviceCEIID),
            "cadmessageid": str(CADmessageId).replace("{", '').replace("}", ''),
            "category": str(category),
            "subtype": str(subtype),
            "source": str(source),
            "message": str(message).replace("|||", '"'),
        }
        logging.info(f"Log Data - {log}")
        LogData.append({"Data": json.dumps(log).encode()})
        result = client.put_record_batch(
            DeliveryStreamName="CEI-logStream", Records=LogData
        )
        logging.info(f"Log Result = {result}")
    except Exception as e:
        logging.error(f"Error - {e}")


def getAgencyName(siteId, agencyId):
    """Get the name for the associated agency

    Args:
        siteId (string): id of installation
        agencyId (string): id of agency

    Returns:
        string: CEIAgencyName
    """
    env = "Stage"
    urlAction = f"/groups/%2f{siteId}%2f{agencyId}"
    urlComplete = f"{url}/{env}/{urlAction}"
    agency = requests.get(urlComplete, headers).json()
    if agency:
        return agency["attributes"]["CEIAgencyName"]
    return "None"


def getDeviceId(CEIDevId, siteId, agencyId):
    """Get the name for the associated vehicle

    Args:
        CEIDevId (string): CEIid of device
        siteId (string): id of installation
        agencyId (string): id of agency

    Returns:
        string: deviceId
    """
    env = "Stage"
    urlAction = f"/groups/%2f{siteId}%2f{agencyId}/ownedby/devices"
    urlComplete = f"{url}/{env}/{urlAction}"
    vehicles = requests.get(urlComplete, headers).json().get("results")
    if vehicles:
        for veh in vehicles:
            if veh["attributes"]["CEIDeviceID"] == CEIDevId:
                return veh["deviceId"]
    return "None"
