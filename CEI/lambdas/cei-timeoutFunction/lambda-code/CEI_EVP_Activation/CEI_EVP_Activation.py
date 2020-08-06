import boto3
import json
import time
from datetime import datetime
import sys, os
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
import logging
from CEI_EVP_Logging.CEI_EVP_Logging import postLog

dynamodb = boto3.resource("dynamodb", region_name="us-west-2")
iotClient = boto3.client("iot-data", region_name="us-west-2")


def activateVehicle(siteId, agencyId, deviceId, vehicleSN, CADmsgId, activate):
    """Primary vehicle control function

    Args:
        siteId (uuid): Region Identifier - used in topic, msgIdx determination
        vehicleSN (string): Vehicle Identifier - used in topic construction
        activate (bool): Enable/Disable probe - used in priority control

    Returns:
        string: IoT Publish result
    """

    """
        Reserve msgIdx - Five attempts before failing
        """
    msgLogPosted = False
    attempts = 0
    idx = 1
    while not msgLogPosted and attempts < 5:
        idx = getMsgIdx(siteId)

        if not isinstance(idx, int):
            postLog(siteId, agencyId, deviceId, CADmsgId,"Error","Message Index Acquisition","CEI EVP Activation", idx )
            break
        logging.info(f"IDX found - {idx} ")
        postMsgResults = postMsgLog(siteId, idx)

        logging.info(f"Post results - {postMsgResults} ")

        if "Error" in postMsgResults:
            attempts += 1
        else:
            logging.info("Idx Reserved")
            msgLogPosted = True

    if not msgLogPosted:
        logging.info("Count not post msgLog")
        postLog(siteId, agencyId, deviceId, CADmsgId,"Error","Message Index Processing","CEI EVP Activation", responseBody )
        return idx

    """
        Construct Data Packet
        """
    commandId = 49
    messageId = idx.to_bytes(4, byteorder="big")
    msgData = 0 if activate else 1
    checksum = (
        commandId + messageId[0] + messageId[1] + messageId[2] + messageId[3] + msgData
    ).to_bytes(2, byteorder="big")
    messageArray = [
        commandId,
        0,
        0,
        0,
        0,
        messageId[0],
        messageId[1],
        messageId[2],
        messageId[3],
        msgData,
        checksum[0],
        checksum[1],
    ]

    """
        Construct IoT Topic, publish
        """
    response = iotClient.publish(
        topic=f"GTT/{siteId}/SVR/EVP/2100/{vehicleSN}/STATE",
        qos=0,
        payload=bytearray(messageArray),
    )
    postLog(siteId, agencyId, deviceId, CADmsgId,"Info","IoT Message Processing","CEI EVP Activation", response )
    return response


def getMsgIdx(siteId):
    """Get the current message index for the site

    Args:
        siteId (uuid): id of site being referenced

    Returns:
        (integer): index of curret site message
    """
    idx = 1
    # Get msgIdx - if new Site, create new entry
    table = dynamodb.Table("cei-iot-msg-idx")
    response = table.query(KeyConditionExpression=Key("siteId").eq(siteId))
    if response.get("Count", 0) == 0:
        try:
            entry = {"siteId": siteId, "idx": idx}
            table.put_item(Item=entry)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logging.error(
                f"400 Error - MsgIdx Processing failed (put) {e} {exc_type} {fname} {exc_tb.tb_lineno}... "
            )
            return f"400 Error - MsgIdx Processing failed (put) {e} {exc_type} {fname} {exc_tb.tb_lineno}... "

    else:
        logging.info(response.get("Items", None)[0])
        idx = response.get("Items", None)[0].get("idx", "derp") + 1
        logging.info(f"MsgIdx found {idx}")

    return int(idx)


def postMsgLog(siteId, idx):
    """
    Update site idx in DynamoDB
    Args:
        siteId (uuid): ID for Installation being updated
        idx (integer): message indexc being updated to

    Returns:
        response (string): dynamo operation result
    """
    logging.info("Posting message log...")
    msglog = {
        "msgId": f"{siteId}_{idx}",
        "timestamp": int(time.time()),
        "responseCode": "TDB",
        "lifeSpan": int(time.time()) + 1200,
    }
    try:
        table = dynamodb.Table("cei-iot-msg-log")
        response = table.put_item(
            Item=msglog, ConditionExpression="attribute_not_exists(msgId)"
        )

        UpdateIdxTable(siteId, idx)

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            response = f"Error - MsgIdx reempted - retrying... ({e})"
            logging.error(response)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        response = f"400 Error - MsgIdx Processing failed (put) {e} {exc_type} {fname} {exc_tb.tb_lineno}..."
        logging.error(response)

    return response


def UpdateIdxTable(siteId, idx):
    """ Update dynamoDB table

    Args:
        siteId (uuid): Region to update the idx for
        idx (integer): value of new IDX

    Returns:
        (string): results of operations
    """
    table = dynamodb.Table("cei-iot-msg-idx")
    response = table.update_item(
        Key={"siteId": siteId,},
        UpdateExpression="SET idx = :val0",
        ExpressionAttributeValues={":val0": idx},
        ReturnValues="UPDATED_NEW",
    )
    return response
