import json
import base64
import boto3
from boto3.dynamodb.conditions import Key
import requests
import time
from datetime import datetime
from CEI_EVP_Activation.CEI_EVP_Activation import activateVehicle
import os
import logging
from CEI_EVP_Logging.CEI_EVP_Logging import postLog

url = os.environ["cdfUrl"]
headers = {
    "Accept": "application/vnd.aws-cdf-v2.0+json",
    "Content-Type": "application/vnd.aws-cdf-v2.0+json",
}



def EVPTimeout(event, context):
    """System Wide Timeout processing - run through each agency and check for last referenced on each vehicle

    Args:
        event (dictionary):  incidental - AWS formatting not used by function
        context (dictionary):  incidental - AWS formatting not used by function

    Returns:
        [type]: [description]
    """
    env = "Stage"


    urlAction = f"/search?type=ceiagency"
    body = ""
    urlComplete = f"{url}/{env}/{urlAction}"


    """
    Get the agency information. 
    """
    code = requests.get(urlComplete, headers).json()
    for agency in code["results"]:
        timeout = agency["attributes"]["CEIEVPTimeoutSecs"]
        logging.info(timeout)
        agencyId = agency["name"]
        regionId = agency["attributes"]["CEISiteIDGuid"]

        urlAction = f"/groups/%2f{regionId}%2f{agencyId}/ownedby/devices"
        vehicleQueryUrl = f"{url}/{env}/{urlAction}"
        vehicles = requests.get(vehicleQueryUrl, headers).json()
        vehCount = len(vehicles["results"])
        if (vehicles):
            for veh in vehicles["results"]:
                try:
                    lastCalled = float(veh["attributes"]["CEILastReferenced"])
                    elapsedSeconds = round((time.time() - lastCalled))
                    if elapsedSeconds > timeout:
                        
                        mqttResult = activateVehicle(
                            regionId, agencyId, veh["attributes"]["CEIDeviceID"],veh["attributes"]["gttSerial"], 'None',False
                        )
                        
                        veh["attributes"]["CEILastReferenced"] = f"{time.time()}"
                        updateAction = f"devices/{veh['deviceId']}"
                        updateUrl = f"{url}/{env}/{updateAction}"
                        patch = {
                            "attributes": {
                                "CEILastReferenced": f"{time.time()}",
                                "CEIVehicleActive": False,
                            }
                        }
                        updateCall = json.dumps(patch).replace("'", '"')
                        logging.info(f"URL COMPLETE: {updateUrl}")
                        logging.info(f"UPDATE CALL: {updateCall}")

                        patchResult = requests.patch(
                            updateUrl, data=updateCall, headers=headers
                        )
                        logging.info(patchResult)
                        postLog(regionId, agencyId, veh['attributes']['CEIDeviceID'], 'None', 'Info', "Timeout Operation", "CEI EVP Timeout", "Vehicle has timed out")
                except Exception as e:
                    postLog(regionId, agencyId, veh['attributes']['CEIDeviceID'], 'None', 'Error', "Processing Exception", "CEI EVP Timeout", e)
                    logging.error(f"ERROR {e}")

    return code
