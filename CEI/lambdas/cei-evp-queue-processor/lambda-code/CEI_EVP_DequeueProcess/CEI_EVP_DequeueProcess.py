import json
import base64
import boto3
from boto3.dynamodb.conditions import Key
import requests
import time
from datetime import datetime
from CEI_EVP_Activation.CEI_EVP_Activation import activateVehicle
from CEI_EVP_IncidentProcess.CEI_EVP_IncidentProcess import postIncident
from CEI_EVP_Logging.CEI_EVP_Logging import postLog
import uuid
import os
import logging

url = os.environ["cdfUrl"]
headers = {
    "Accept": "application/vnd.aws-cdf-v2.0+json",
    "Content-Type": "application/vnd.aws-cdf-v2.0+json",
}


def queueHandler(event, context):
    """Dequeue event from sqs for processing

    Args:
        event (dictionary): event args
        context (content ): [description]
    """
    for record in event["Records"]:
        payload = record["body"]
        EVPDeterminationProcess(json.loads(payload))


def determinanceExecutor(code):
    """Runs the specific agency evp determination boolean

    Args:
        code (string): boolean code for EVP for the determinance for the given agency
    Returns:
        i: result
    """
    exec("global i; i = %s" % code)
    global i
    return i


def EVPDeterminationProcess(event):
    """Main determination process - process incident, update vehicle status in CDF, determine & set vehicle activation status

    Args:
        event (dictionary): incident details

    Returns:
        processResult (json string): results of all operations.
    """

    # global reference necessary for running agency boolean code
    global determine
    logging.info(f'Begin Dequeue Process for {event["messageId"]}')

    # URL Components - Calls to CDF for vehicle/agency info.
    env = "Stage"
    siteId = event["siteId"]
    agencyId = event["agencyId"]
    urlAction = f"/groups/%2f{siteId}%2f{agencyId}"
    urlComplete = f"{url}/Stage/{urlAction}"

    # Response Components
    responseCode = 202
    responseBody = ""

    """
    Get the agency information. 
    """
    code = requests.get(urlComplete, headers).json()

    """
    Handle Incident Tracking 
    """
    postRes = postIncident(event)
    if isinstance(type(postRes), str):
        responseCode = 500
        if postRes[0] == "4":
            responseCode = 400
        response = {"statusCode": responseCode, "body": postRes}
        return response
    logging.info("Incident Processing...")
    logging.info(f"{postRes}")

    incidentValue = postRes
    logging.info(f"{code}")
    responseBody = "Agency Found ... "
    if not code.get("attributes"):
        responseCode = 500
        response = {"statusCode": responseCode, "body": "Error - Agency not found"}
        return response

    agencyConditional = f"{code['attributes']['CEIEVPConditional']}"
    logging.info(f"Agency Conditional Determined - {agencyConditional}")
    responseBody += f"Agency Conditional Determined - {agencyConditional} ... "

    for i in range(len(event["incidents"])):
        if event["incidents"][i].get("units") is None:
            postLog(siteId, agencyId, "None", event["messageId"],"Info","Dequeue Determination","CEI EVP Dequeue", responseBody )
            break
        for unit in event["incidents"][i]["units"]:
            deviceId = getDeviceId(unit["deviceId"], event["siteId"], event["agencyId"],event["messageId"])
            logging.info(f"Translated Device ID - {deviceId}")
            urlAction = f"devices/{deviceId}"
            urlComplete = f"{url}/{env}/{urlAction}"
            logging.info(f"\r\nFinding status for {deviceId}...\r\n")
            try:
                vehicleStatus = requests.get(urlComplete, headers)
                if vehicleStatus.status_code == 200:
                    responseBody += f"Vehicle {deviceId} found ... "
                    vehicleStatus = vehicleStatus.json()
                    logging.info(f"{vehicleStatus}")

                    logging.info("\n Updating...")
                    vehicleStatus["attributes"]["CEIIncidentStatus"] = incidentValue[
                        "incidentStatus"
                    ]
                    vehicleStatus["attributes"][
                        "CEIIncidentStatusDateandTime"
                    ] = incidentValue["incidentStatusDateTime"]
                    vehicleStatus["attributes"]["CEIIncidentTypeCode"] = incidentValue[
                        "incidentType"
                    ]
                    vehicleStatus["attributes"]["CEIIncidentAction"] = incidentValue[
                        "incidentAction"
                    ]
                    vehicleStatus["attributes"]["CEIIncidentPriority"] = incidentValue[
                        "incidentPriority"
                    ]
                    vehicleStatus["attributes"][
                        "CEIIncidentLocationName"
                    ] = incidentValue["incidentLocationName"]
                    vehicleStatus["attributes"][
                        "CEIIncidentActionDateandTime"
                    ] = incidentValue["incidentActionDateTime"]
                    vehicleStatus["attributes"][
                        "CEIIncidentLocationCrossStreet"
                    ] = incidentValue["incidentLocationCrossStreet"]
                    vehicleStatus["attributes"][
                        "CEIIncidentLocationStreet1"
                    ] = incidentValue["incidentLocationStreet1"]
                    vehicleStatus["attributes"][
                        "CEIIncidentLocationStreet2"
                    ] = incidentValue["incidentLocationStreet2"]
                    vehicleStatus["attributes"][
                        "CEIIncidentLocationCity"
                    ] = incidentValue["incidentLocationCity"]
                    vehicleStatus["attributes"][
                        "CEIIncidentLocationState"
                    ] = incidentValue["incidentLocationState"]
                    vehicleStatus["attributes"][
                        "CEIIncidentLocationCounty"
                    ] = incidentValue["incidentLocationCounty"]
                    vehicleStatus["attributes"][
                        "CEIIncidentLocationZip"
                    ] = incidentValue["incidentLocationZip"]
                    vehicleStatus["attributes"][
                        "CEIIncidentLocationDirections"
                    ] = incidentValue["incidentLocationDirections"]
                    vehicleStatus["attributes"][
                        "CEIIncidentLocationCoordinates"
                    ] = incidentValue["incidentLocationCoordinates"]

                    vehicleStatus["attributes"]["CEIUnitID"] = unit.get(
                        "unitId", vehicleStatus["attributes"]["CEIUnitID"]
                    )
                    vehicleStatus["attributes"]["CEIDispatchDateandTime"] = unit.get(
                        "dispatchDateTime",
                        vehicleStatus["attributes"]["CEIDispatchDateandTime"],
                    )
                    vehicleStatus["attributes"]["CEIUnitStatus"] = unit.get(
                        "status", vehicleStatus["attributes"]["CEIUnitStatus"]
                    )
                    vehicleStatus["attributes"]["CEIUnitStatusDateandTime"] = unit.get(
                        "statusDateTime",
                        vehicleStatus["attributes"]["CEIUnitStatusDateandTime"],
                    )

                    if len(unit["location"]) > 0:
                        vehicleStatus["attributes"][
                            "CEIUnitLocationLatitudeandLongitude"
                        ] = f"{unit['location']['geometry'].get('coordinates', vehicleStatus['attributes']['CEIUnitLocationLatitudeandLongitude'])}"
                        vehicleStatus["attributes"][
                            "CEIUnitLocationDateandTime"
                        ] = unit["location"].get(
                            "updateDateTime",
                            vehicleStatus["attributes"]["CEIUnitLocationDateandTime"],
                        )

                    vehicleStatus["attributes"]["CEILastReferenced"] = f"{time.time()}"

                    updateCall = (
                        json.dumps(vehicleStatus).replace("'", '"').replace("|||", "'")
                    )
                    logging.info(f"URL COMPLETE: {urlComplete}")
                    logging.info(f"UPDATE CALL: {updateCall}")
                    patchResult = requests.patch(
                        urlComplete, data=updateCall, headers=headers
                    )
                    logging.info(patchResult)
                    if patchResult.status_code == 204:
                        responseBody += f"Updated  {deviceId} status... "
                        logging.info("Attempting Determinance...")
                        determine = vehicleStatus
                        determinanceResult = determinanceExecutor(agencyConditional)
                        logging.info(f"{determinanceResult}")
                        responseBody += f"Priority Set to {determinanceResult}... "

                        mqttResult = activateVehicle(
                            siteId,
                            agencyId,
                            unit["deviceId"],
                            vehicleStatus["attributes"]["gttSerial"],
                            event["messageId"],
                            determinanceResult
                        )

                        updateAction = f"devices/{deviceId}"
                        updateUrl = f"{url}/{env}/{updateAction}"
                        patch = {
                            "attributes": {
                                "CEILastReferenced": f"{time.time()}",
                                "CEIVehicleActive": determinanceResult,
                            }
                        }
                        updateCall = json.dumps(patch).replace("'", '"')
                        logging.info(f"URL COMPLETE: {updateUrl}")
                        logging.info(f"UPDATE CALL: {updateCall}")

                        patchResult = requests.patch(
                            updateUrl, data=updateCall, headers=headers
                        )
                        logging.info(patchResult)
                else:
                    logging.info(f"Status for {deviceId} not found")
                    responseBody += f"Status for {deviceId} not found... "
                
                postLog(siteId, agencyId, unit["deviceId"], event["messageId"],"Info","Dequeue Determination","CEI EVP Dequeue", responseBody )        

            except Exception as e:
                logging.error(f"Error {e}")
                responseBody += f"Error on {deviceId} : {e} ... "
                postLog(siteId, agencyId, unit["deviceId"],event["messageId"],"Error","Dequeue Determination","CEI EVP Dequeue", responseBody )
                responseCode = 500
    
    
    return {"statusCode": responseCode, "body": responseBody}


def getDeviceId(CEIDevId, siteId, agencyId, messageId):
    """Translate from CEI Device ID to CDF Device ID

    Args:
        CEIDevId (string): CEI Device ID being referenced
        siteId (uuid): Installation id being reference
        agencyId (uuid): agency id being referenced

    Returns:
        deviceId: CDF id for vehicle being referenced
    """
    # URL Components
    env = "Stage"
    urlAction = f"/groups/%2f{siteId}%2f{agencyId}/ownedby/devices"
    urlComplete = f"{url}/{env}/{urlAction}"
    vehicles = requests.get(urlComplete, headers).json().get("results", "")

    if len(vehicles) > 0:
        for veh in vehicles:
            if veh["attributes"]["CEIDeviceID"] == CEIDevId:
                return veh["deviceId"]
    postLog(siteId, agencyId, CEIDevId,messageId,"Error","Dequeue DeviceId Determination","CEI EVP Dequeue", "ERROR - DeviceID not found" )
    return "ERROR - DeviceID not found"
