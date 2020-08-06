import json
import base64
import boto3
from boto3.dynamodb.conditions import Key
import requests
import time
from datetime import datetime
import logging
import os
from CEI_EVP_Logging.CEI_EVP_Logging import postLog
dynamodb = boto3.resource("dynamodb", region_name="us-west-2")

# url components
url = os.environ["cdfUrl"]
headers = {
    "Accept": "application/vnd.aws-cdf-v2.0+json",
    "Content-Type": "application/vnd.aws-cdf-v2.0+json",
}


def postIncident(event):
    """Record the incoming incident details to dynamodb

    Args:
        event (dictionary): Incident Event details: used for storing/updating incident record

    Returns:
        response(string): results of all operations
    """

    logging.info("Incident Tracking...")
    """
    process incidents within events - "None" used as CDF friendly default.
    """
    for i in range(len(event["incidents"])):
        # incident values to be updated
        crossStreet = "None"
        street1 = "None"
        street2 = "None"
        city = "None"
        state = "None"
        county = "None"
        zipCode = "None"
        directions = "None"
        coordinates = "None"
        try:
            inc_id = event["incidents"][i].get("id", "None")
            status = event["incidents"][i].get("status", "None")
            statusDateTime = event["incidents"][i].get("statusDateTime", "None")
            typeCode = event["incidents"][i].get("type", "None")
            priority = event["incidents"][i].get("priority", "None")
            action = event["incidents"][i].get("action", "None")
            actionDateTime = event["incidents"][i].get("actionDateTime", "None")
            locationDict = event["incidents"][i].get("location", {})
            locationName = locationDict.get("name", "None")
            locAddrDict = locationDict.get("address", {})
            if len(locAddrDict) > 0:
                crossStreet = locAddrDict.get("crossStreet", "None")
                street1 = locAddrDict.get("street1", "None")
                street2 = locAddrDict.get("street2", "None")
                city = locAddrDict.get("city", "None")
                state = locAddrDict.get("state", "None")
                county = locAddrDict.get("county", "None")
                zipCode = locAddrDict.get("zip", "None")
                directions = locAddrDict.get("directions", "None")
                if len(locationDict["geometry"]) > 0:
                    coordinates = (
                        f"{ locationDict['geometry'].get('Coordinates','None') }"
                    )
        except Exception as e:
            postLog(event.get("siteId", "None"),event.get("agencyId", "None"),"None",event.get("messageId", "None"), "Error", "Incident Processing", "New Incident",  f"400 Error - Incident Processing failed (parsing) {e}... ")
            return f"400 Error - Incident Processing failed (parsing) {e}... "

    # incident declaration - "None" used for CDF purposes
    incident = {
        "IncidentID": (inc_id if inc_id != "" else "None"),
        "incidentStatus": (status if status != "" else "None"),
        "incidentStatusDateTime": (statusDateTime if statusDateTime != "" else "None"),
        "incidentType": (typeCode if typeCode != "" else "None"),
        "incidentPriority": (priority if priority != "" else "None"),
        "incidentActionDateTime": (actionDateTime if actionDateTime != "" else "None"),
        "incidentLocationName": (
            locationName if locationName != "" else "None"
        ).replace("'", "|||"),
        "incidentLocationCrossStreet": (
            crossStreet if crossStreet != "" else "None"
        ).replace("'", "|||"),
        "incidentLocationStreet1": (street1 if street1 != "" else "None").replace(
            "'", "|||"
        ),
        "incidentLocationStreet2": (street2 if street2 != "" else "None").replace(
            "'", "|||"
        ),
        "incidentLocationCity": (city if city != "" else "None").replace("'", "|||"),
        "incidentLocationState": (state if state != "" else "None"),
        "incidentLocationCounty": (county if county != "" else "None").replace(
            "'", "|||"
        ),
        "incidentLocationZip": (zipCode if zipCode != "" else "None"),
        "incidentLocationDirections": (
            directions if directions != "" else "None"
        ).replace("'", "|||"),
        "incidentLocationCoordinates": (coordinates if coordinates != "" else "None"),
        "incidentAction": (action if action != "" else "None"),
        "ceittl": int(time.time()) + 6000,
    }

    for key, value in incident.items():
        logging.info(f"{key} {value}")
        if value == "":
            value = "None"

    response = "DynamoDB Failure"
    try:
        table = dynamodb.Table("CEI-IncidentTracking")
        response = table.query(
            KeyConditionExpression=Key("IncidentID").eq(inc_id)
        )
        if response.get("Count") != 1:
            response = "IncidentID not found - recording... "
            try:
                table.put_item(Item=incident)

            except (
                AccessDeniedException,
                ConditionalCheckFailedException,
                IncompleteSignatureException,
                ItemCollectionSizeLimitExceededException,
                LimitExceededException,
                MissingAuthenticationTokenException,
                ProvisionedThroughputExceededException,
                RequestLimitExceeded,
                ResourceInUseException,
                ResourceNotFoundException,
                ThrottlingException,
                UnrecognizedClientException,
                ValidationException,
            ) as e:
                response = f"500 Error - Incident Processing failed (put) {e}... "
                logging.error(response)
                postLog(event.get("siteId", "None"),event.get("agencyId", "None"),"None",event.get("messageId", "None"), "Error", "Incident Processing", "New Incident", response)
                return response
            except Exception as e:
                response = f"400 Error - Incident Processing failed (put) {e}... "
                logging.error(response)
                postLog(event.get("siteId", "None"),event.get("agencyId", "None"),"None",event.get("messageId", "None"), "Error", "Incident Processing", "New Incident", response)
                return response
            #response = incident
            postLog(event.get("siteId", "None"),event.get("agencyId", "None"),"None",event.get("messageId", "None"), "Info", "Incident Processing", "New Incident", response)
        else:
            logging.info("incident ID found - updating...")
            try:
                responseItems = response.get("Items")[0]
                for k in incident:
                    logging.info(f"{incident[k]} vs {responseItems[k]}")
                    if incident[k] == "None":
                        incident[k] = responseItems[k]
                        logging.info(f"assigned to {incident[k]}")
                """
                Update incident within dynamoDB
                """
                response = table.update_item(
                    Key={"IncidentID": incident["IncidentID"],},
                    UpdateExpression="SET ceittl = :val0, incidentStatus = :val1, incidentStatusDateTime = :val2,  incidentType = :val3,  incidentPriority = :val4,  incidentActionDateTime = :val5,  incidentLocationName = :val6,  incidentLocationCrossStreet = :val7,  incidentLocationStreet1 = :val8,  incidentLocationStreet2 = :val9,  incidentLocationCity = :val10,  incidentLocationState = :val11,  incidentLocationCounty = :val12,  incidentLocationZip = :val13,  incidentLocationDirections = :val14,  incidentLocationCoordinates = :val15, incidentAction = :val16",
                    ExpressionAttributeValues={
                        ":val0": incident["ceittl"],
                        ":val1": incident["incidentStatus"],
                        ":val2": incident["incidentStatusDateTime"],
                        ":val3": incident["incidentType"],
                        ":val4": incident["incidentPriority"],
                        ":val5": incident["incidentActionDateTime"],
                        ":val6": incident["incidentLocationName"],
                        ":val7": incident["incidentLocationCrossStreet"],
                        ":val8": incident["incidentLocationStreet1"],
                        ":val9": incident["incidentLocationStreet2"],
                        ":val10": incident["incidentLocationCity"],
                        ":val11": incident["incidentLocationState"],
                        ":val12": incident["incidentLocationCounty"],
                        ":val13": incident["incidentLocationZip"],
                        ":val14": incident["incidentLocationDirections"],
                        ":val15": incident["incidentLocationCoordinates"],
                        ":val16": incident["incidentAction"],
                    },
                    ReturnValues="UPDATED_NEW",
                )
                logging.info(f"Update response is {response}")
                response = incident
                postLog(event.get("siteId", "None"),event.get("agencyId", "None"),"None",event.get("messageId", "None"), "Info", "Incident Processing", "Update Incident", f"Update response is {response}")
            except (
                AccessDeniedException,
                ConditionalCheckFailedException,
                IncompleteSignatureException,
                ItemCollectionSizeLimitExceededException,
                LimitExceededException,
                MissingAuthenticationTokenException,
                ProvisionedThroughputExceededException,
                RequestLimitExceeded,
                ResourceInUseException,
                ResourceNotFoundException,
                ThrottlingException,
                UnrecognizedClientException,
                ValidationException,
            ) as e:
                response = f"500 Error - Incident Processing failed (put) {e}... "
                logging.error(response)
                postLog(event.get("siteId", "None"),event.get("agencyId", "None"),"None",event.get("messageId", "None"), "Error", "Incident Processing", "Update Incident", response)
            except Exception as e:
                return f"400 Error - Incident Processing failed (update) {e}... "
                logging.error(response)
                postLog(event.get("siteId", "None"),event.get("agencyId", "None"),"None",event.get("messageId", "None"), "Error", "Incident Processing", "Update Incident", response)
                
    except Exception as e:
        response = f"400 Error - Incident Processing failed {e}... "
    return response
