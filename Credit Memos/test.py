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
# ------------------------------------------------
# connection to database
server = dbserver
database = dbname
username = 'sa'
password = dbpass


try:
    # conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
    #                       server+';DATABASE='+database+';Trusted_Connection=yes;')
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                          server+';DATABASE='+database+';UID='+username+';PWD=' + password)
    cursor = conn.cursor()
except:
    print(
        f"Connecting To {server} and database {database} failed. Check Password and User Credentials ")
# ---------------------


def taxSummary(docno):
    sql = ''' select h.netamount, h.taxamount, h.grossamount, COUNT(b.invnumber) from [_cplURACreditMemo] h inner 
    join _cplUraInvLines b on h.docno = b.invnumber where h.docno = ? AND b.itemprice > 0
    group by h.netamount, h.taxamount, h.grossamount '''
    values = (docno)
    cursor.execute(sql, values)
    for row in cursor:
        summary1 = {
            "netAmount": f"{row[0]}",
            "taxAmount": f"{row[1]}",
            "grossAmount": f"{row[2]}",
            "itemCount": f"{row[3]}",
            "modeCode": "0",
            "remarks": "",
            "qrCode": ""
        }
    print(summary1)
    return summary1


taxSummary('CRN-E0014')
