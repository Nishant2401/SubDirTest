import json
import requests as req
import os

base_url = os.environ['BASE_URL']
headers = {
    'Accept': 'application/vnd.aws-cdf-v2.0+json',
    'Content-Type': 'application/vnd.aws-cdf-v2.0+json'
}

def lambda_handler(event, context):
    att_str = '{"attributes":{'
    orig_len = len(att_str)
    att_end = '}}'
    
    time = list(event.keys())[0] 
    data = event.get(time)
    
    # Get modem serial number
    # Sample topic: N684570206021035/messages/json
    SN = event.get('topic').split('/')[0]
    
    # Assemble URL
    url = f'{base_url}/devices/{SN.lower()}'

    # Store data in local vars, set to "No Data" if key not found
    # Note: no message has all the data saved here 
    lat = data.get('atp.glat') # Current latitude
    lon = data.get('atp.glon') # Current longitude
    hdg = data.get('atp.ghed') # Current heading
    spd = data.get('atp.gspd') # Current speed kmph
    stt = data.get('atp.gstt') # GPS fix status (0 = no fix, 1 = fix)
    sat = data.get('atp.gsat') # Number of satellites at time of message
    gpi = data.get('atp.gpi')  # GPIO state - bit masked

    if stt != None:
        att_str += f'"fixStatus":"{stt}",'
        
    if sat != None:
        att_str += f'"numFixSat":"{sat}",'
        
    if lat != None:
        att_str += f'"lat":"{lat}",'
        
    if lon != None:
        att_str += f'"long":"{lon}",'
        
    if hdg != None:
        att_str += f'"heading":"{hdg}",'
        
    if spd != None:
        att_str += f'"speed":"{spd}",'
        
    if gpi != None:
        att_str += f'"auxiliaryIo":"{gpi}",' 
        att_str += f'"lastGPIOMsgTimestamp":"{time}",'
        
    if stt != None or sat != None or lat != None or lon != None or hdg != None or spd != None:
        att_str += f'"lastGPSMsgTimestamp":"{time}",'
    
    # only PATCH if data to patch
    if len(att_str) > orig_len:
        # remove comma from end of string
        post_data = att_str[:-1] + att_end
        req.patch(url, data=post_data, headers=headers)