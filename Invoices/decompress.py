import requests
import datetime
import base64
import json
import gzip
import pyodbc
import pyqrcode
import png
import time
from datetime import datetime as dt
import os
import os.path
import schedule
from configparser import ConfigParser


# LOAD THE CONFIG FILE
config = ConfigParser()
config.read('config.ini')

tin = (config['info']['tin'])
deviceno = (config['info']['deviceno'])
mac = (config['info']['mac'])
uuid = (config['info']['uuid'])
legalname = (config['info']['legalname'])
email = (config['info']['email'])
longitude = (config['info']['longitude'])
latitude = (config['info']['latitude'])
address = (config['info']['address'])
mobilephone = (config['info']['mobilephone'])
placeofbusiness = (config['info']['placeofbusiness'])
dbname = (config['info']['database'])
dbpass = (config['info']['password'])
dbserver = (config['info']['server'])
qrcodefolder = (config['info']['qrcodefolder'])
# ---------------------------------------------------------


# url = "http://127.0.0.1:9880/efristcs/ws/tcsapp/getInformation"
url = "https://efristest.ura.go.ug/efrisws/ws/taapp/getInformation"
headers = {
    'Content-Type': 'application/json'
}

# Logging Function and the creation of the log folders


def unzipJson(txt):
    decorded_cont = base64.b64decode(txt)
    unzipped_cnt = gzip.decompress(decorded_cont).decode("utf-8")
    return json.loads(unzipped_cnt)


def logPayload(log):
    now = dt.now()
    date1 = now.strftime("%Y-%m-%d")
    f = open(f".\logs\Payload {date1}"+".txt", "a")
    f.write("{0} -- {1}\n".format(dt.now().strftime("%Y-%m-%d %H:%M:%S"), log))
    f.close()


def logSummary(log):
    now = dt.now()
    date1 = now.strftime("%Y-%m-%d")
    f = open(f".\logs\Summary {date1}"+".txt", "a")
    f.write("{0} -- {1}\n".format(dt.now().strftime("%Y-%m-%d %H:%M:%S"), log))
    f.close()


def logURA(log):
    now = dt.now()
    date1 = now.strftime("%Y-%m-%d")
    f = open(f".\logs\Response {date1}"+".txt", "a")
    f.write("{0} -- {1}\n".format(dt.now().strftime("%Y-%m-%d %H:%M:%S"), log))
    f.close()


if not os.path.exists(".\logs"):
    os.makedirs(r".\logs")
# -------------------------------------------------


# connection to database
server = dbserver
database = dbname
username = 'sa'
password = dbpass


def sendInvoice():
    nowdt = datetime.datetime.now()
    dt_plus_2 = nowdt + datetime.timedelta(minutes=2)
    dt = dt_plus_2.strftime("%Y-%m-%d %H:%M:%S")
    # creation of the payload with the content
    payload = "{\r\n    \"data\": {\r\n \"content\": \"\",\r\n \"signature\": \"\",\r\n  \"dataDescription\": {\r\n  \"codeType\": \"0\",\r\n \"encryptCode\": \"0\",\r\n                     \"zipCode\": \"0\"\r\n        }\r\n    },\r\n    \"globalInfo\": {\r\n        \"appId\": \"AP04\",\r\n        \"version\": \"1.1.20191201\",\r\n        \"dataExchangeId\": \"%s\",\r\n        \"interfaceCode\": \"T115\",\r\n        \"requestCode\": \"TP\",\r\n        \"requestTime\": \"%s\",\r\n        \"responseCode\": \"TA\",\r\n        \"userName\": \"admin\",\r\n        \"deviceMAC\": \"%s\",\r\n        \"deviceNo\": \"%s\",\r\n        \"tin\": \"%s\",\r\n        \"brn\": \"\",\r\n        \"taxpayerID\": \"%s\",\r\n        \"longitude\": \"%s\",\r\n        \"latitude\": \"%s\"\r\n    },\r\n    \"returnStateInfo\": {\r\n        \"returnCode\": \"\",\r\n        \"returnMessage\": \"\"\r\n    }\r\n}" % (
        uuid, dt, mac, deviceno, tin, tin, longitude, latitude)

    headers = {'Content-Type': 'application/json'}

    print(payload)

    response = requests.request(
        "POST", url, headers=headers, data=payload)  # make request

    # responses.append(json.loads(response.text))
    # print(response.text)
    invoiceres = json.loads(response.text)
    logURA(invoiceres)
    encryptionType = invoiceres['data']['dataDescription']['zipCode']
    # CHECKS THE RETURN STATUS AND DOES THE APPROPRIATE ACTIONS
    if invoiceres['returnStateInfo']['returnMessage'] == 'SUCCESS':
        if encryptionType == '1':
            ress = invoiceres['data']['content']
            content_decoded = unzipJson(ress)
            logSummary(ress)
        elif encryptionType == '0':
            content = invoiceres['data']['content']
            content_decoded = base64.b64decode(content.encode('utf-8'))
            content_decoded = json.loads(content_decoded.decode('utf-8'))


sendInvoice()
