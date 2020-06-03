import json
import base64
import boto3
import requests as req
import binascii
import os 

# Create client here so that it will stay 'warm' between invocations saving execution time
client = boto3.client('iot-data', os.environ['AWS_REGION'])
base_url = os.environ['BASE_URL']
headers = {
    'Accept': 'application/vnd.aws-cdf-v2.0+json',
    'Content-Type': 'application/vnd.aws-cdf-v2.0+json'
}

def convert_lat_lon_to_minutes_degrees(lat, lon):
    returnLat = to_min_degrees(lat)
    returnLon = to_min_degrees(lon)
    return returnLat, returnLon

def to_min_degrees(data):
    ddd = int(data)
    mmmmmm = float(data - float(ddd)) * 60
    data = (ddd*1000000 + int(mmmmmm*10000))/1000000
    newString = hex(int(data * 1000000))
    intdata = int(newString,16)
    returnData = intdata.to_bytes(4,byteorder="little",signed=True)
    return returnData

def convert_speed(speed):
    newSpeed = int(speed * 25 / 18 + 0.5)
    return newSpeed.to_bytes(1,byteorder="little",signed=False)

def convert_heading(heading):
    newHeading = int(heading / 2 + 0.5)
    return newHeading.to_bytes(1,byteorder="little",signed=False)

def lambda_handler(event, context):
    #default
    vehSN = b'\xd5\x23\x83\x00'
    # MP70 does not provide RSSI information set to 254
    vehRSSI = b'\x00\xFE'
    # filler
    vehPad1 = b'\x00\x00'
    vehGPSVel_mpsd5 = b'\x64'
    vehGPSHdg_deg2 = b'\x58'
    gpsStatus = 0 
    # CMS can go to CDF to get this info directly		
    vehGPSSatellites = b'\x00\x00\x00\x00'
    vehVehID = b'\x1f\x00'
    vehCityID = b'\x01'
    opStatus = 0
    vehClass = b'\x06'                        
    # always 0 
    conditionalPriority = b'\x00'
    # filler
    vehPad2 = b'\x00\x00'
    # modem has no slot
    vehDiagValue = b'\x00\x00\x00\x00'
    
    pub_topic = event.get('topic')
    message = event.get('data')

    # make sure topic is valid 
    if 'messages/json' in pub_topic:
        # get SN from topic 
        # Topic is always SN/messages/json where SN is the serial number of the modem
        splitTopic = pub_topic.split('/')
        clientID = splitTopic[0]

        # Assemble URL for device
        url = f'{base_url}/devices/{clientID.lower()}'
        
        # get data device from CDF
        code = req.get(url, headers=headers)
        
        # Only work with good data
        if code:
            dataCDF = code.json()
            
            # get region and agency name from device CDF data
            groups = dataCDF.get('groups')
            if groups != None:
                out = groups.get('out')
            else: 
                raise Exception("No groups in CDF data")
                
            if out != None: 
                ownedby = out.get('ownedby')
            else: 
                raise Exception("No groups/out in CDF data")
                
            if ownedby != None:
                agency = ownedby[0].split('/')[2]
                region = ownedby[0].split('/')[1]
            else: 
                raise Exception("No groups/out/ownedby in CDF data")

            # get attributes from device CDF data
            attributes = dataCDF.get('attributes')
            if attributes == None:
                raise Exception("No attributes value in json string")
            
            gttSerial = attributes.get('gttSerial')
            if gttSerial == None:
                raise Exception("No gttSerial value in attributes")

            addressMAC = attributes.get('addressMAC')
            if addressMAC == None:
                raise Exception("No addressMAC value in attributes")

            # use device data to get agency data
            url = f'{base_url}/groups/%2f{region}%2f{agency}'

            code = req.get(url, headers=headers)

            dataAgency = code.json()
            attributesAgency = dataAgency.get('attributes')

            agencyCode = attributesAgency.get('agencyCode')
            if agencyCode == None:
                raise Exception('Agency Code not found in CDF')

            # use device data to get region data
            url = f'{base_url}/groups/%2f{region}'

            code = req.get(url, headers=headers)

            dataRegion = code.json()
            attributesRegion = dataRegion.get('attributes')

            regionGUID = attributesRegion.get('regionGUID')
            if regionGUID == None:
                raise Exception("No region GUID found in CDF, please check that entity exists and is correct")
            
            # parse new data from incoming GPS message
            # Epoch time is used for the key 
            time = list(event.keys())[0] 
            
            dataGPS = event.get(time)
            
            # Store data in local vars, set to "No Data" if key not found
            # Note: no message has all the data saved here 
            lat = dataGPS.get('atp.glat') # Current latitude
            lon = dataGPS.get('atp.glon') # Current longitude
            hdg = dataGPS.get('atp.ghed') # Current heading
            spd = dataGPS.get('atp.gspd') # Current speed kmph
            stt = dataGPS.get('atp.gstt') # GPS fix status (0 = no fix, 1 = fix)
            gpi = dataGPS.get('atp.gpi')  # GPIO state - bit masked
            
            # process GPS message and create RTRADIO message
            if lat != None and lon != None:
                # incoming latitude needs to be converted to degrees/minutes because
                # CMS expects it in that format
                vehGPSLat_ddmmmmmm, vehGPSLon_dddmmmmmm = convert_lat_lon_to_minutes_degrees(lat, lon)
                    
                if addressMAC != None:
                    addressMAC = addressMAC.replace(':','')
                    try:
                        addressMAC = addressMAC.zfill(5)
                        vehSN = int("008" + addressMAC[-5:],16).to_bytes(4,byteorder="little",signed=False)
                    except:
                        raise Exception("addressMAC is not numeric")
                
                spdCDF = attributes.get('speed')
                if spd != None:
                    # gspd is km/h coming from the MP70, needs to be converted to 
                    # (meters / 5)/second
                    vehGPSVel_mpsd5 = convert_speed(spd)
                elif spdCDF != None:
                    # get last known speed from CDF stored in km/h format
                    vehGPSVel_mpsd5 = convert_speed(spdCDF)

                hdgCDF = attributes.get('heading')
                if hdg != None:
                    # ghed is heading in degrees coming from the MP70, needs to be converted
                    # to heading per two degrees
                    vehGPSHdg_deg2 = convert_heading(hdg)
                elif hdgCDF != None:
                    # get last known heading from cdf
                    vehGPSHdg_deg2 = convert_heading(hdgCDF)
                    
                if stt != None:
                    # gstt is only 0 or 1, so we don't have 3D or 3D+ as possibilities
                    if stt == 0:
                        gpsStatus &= 0x3FFF
                    else:
                        gpsStatus |= 0xC000
                else:
                    gpsStatus |= 0xC000    

                vehGPSCStat = gpsStatus.to_bytes(2,byteorder="little",signed=False)
                
                # get VID from CDF but default to 0
                VID = attributes.get('VID')
                if VID != None:
                    vehVehID = int(VID).to_bytes(2,byteorder='little',signed=False)
                # get agency id from CDF but default to 1
                if agencyCode != None:
                    vehCityID = agencyCode.to_bytes(1,byteorder="little",signed=False)
                
                # IO Mapping
                # IO 1 == ignition
                # IO 2 == left blinker
                # IO 3 == right blinker
                # IO 4 == light bar
                # IO 5 == disable
                # use new GPIO data if available
                # Get CDF data if not
                if gpi == None:
                    gpi = attributes.get('auxiliaryIo')
                
                gpi = int(gpi)
                # use GPIO information 
                ignition = gpi & 0x01
                leftTurn = (gpi & 0x02) >> 1
                rightTurn = (gpi & 0x04) >> 2
                lightBar = (gpi & 0x08) >> 3
                disable = (gpi & 0x10) >> 4 
                
                # turn status
                turnStatus = leftTurn
                turnStatus |= rightTurn

                # op status
                if ignition == 0 or disable == 1: 
                    opStatus = 0
                elif lightBar == 1 and ignition == 1 and disable == 0:
                    opStatus = 1
                
                # veh mode 
                priority = attributes.get('priority')
                if priority != None:
                    if priority == 'High':
                        vehMode = 0
                    elif priority == 'Low':
                        vehMode = 1
                    else: 
                        vehMode = 1
                else: 
                    vehMode = 1
                # pack up data 
                vehModeOpTurn = turnStatus 
                vehModeOpTurn |= (opStatus << 2)
                vehModeOpTurn |= (vehMode << 5) 
                vehModeOpTurn = vehModeOpTurn.to_bytes(1,byteorder="little",signed=False)

                # get Class from CDF
                vehClass = attributes.get('class')
                if vehClass != None:
                    vehClass = int(vehClass).to_bytes(1,byteorder="little",signed=False)
                
                # assemble message
                messageData = vehSN + vehRSSI + vehPad1 + vehGPSLat_ddmmmmmm + vehGPSLon_dddmmmmmm + \
                    vehGPSVel_mpsd5 + vehGPSHdg_deg2 + vehGPSCStat + vehGPSSatellites + vehVehID + \
                    vehCityID + vehModeOpTurn + vehClass + conditionalPriority + vehPad2 + vehDiagValue

                # out going topic structure "GTT/GTT/VEH/EVP/2100/2100ET0001/RTRADIO"
                # where second GTT is replaced with the region GUID
                newTopic = f'GTT/{regionGUID.lower()}/VEH/EVP/2100/{gttSerial}/RTRADIO'

                # Send out new Topic to CMS associated with the deviceID aka SN
                response = client.publish(topic = newTopic,
                                            qos = 0,
                                            payload = messageData)

        else:
            print(f'Request failed with code: {code.status_code}')

