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


url = "http://127.0.0.1:9880/efristcs/ws/tcsapp/getInformation"
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


try:
    # conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
    #                       server+';DATABASE='+database+';Trusted_Connection=yes;')
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' +
                          server+';DATABASE='+database+';UID='+username+';PWD=' + password)
    cursor = conn.cursor()
except:
    print(
        f"Connecting To {server} and database {database} failed. Check Password and User Credentials ")
# -------------------------------------------
# -------------------------------------------

# allgoodsInfo = []
# summaryTax = []

#  A function that returns the taxSummary section


def TaxTypes(docno):
    taxList = []
    sql = '''select taxCategory, round((round(grossAmount,2)-taxAmount),2) as netAmount, taxRate, round(taxAmount,2), round(grossAmount,2),taxRateName
        from
        (select (case when taxrate=0.18 then '01' when taxrate=0 then '02' else 'NA' end) as taxCategory,  (case when taxrate=0.18 then '18%' when taxrate=0 then '0%' else 'NA' end) as taxRateName,
        sum(round(linetotprice,2)) as grossAmount, sum(round(taxamount,2)) as taxAmount, taxrate as taxRate
        from _cplUraInvLines
        where invnumber= ? and itemprice > 0
        group by taxrate) PP '''
    values = (docno)
    cursor.execute(sql, values)
    for tax in cursor:
        taxtype = {
            "taxCategoryCode": f"{tax[0]}",
            "netAmount": f"{tax[1]}",
            "taxRate": f"{tax[2]}",
            "taxAmount": f"{tax[3]}",
            "grossAmount": f"{tax[4]}",
            "exciseUnit": "",
            "exciseCurrency": "",
            "taxRateName": f"{tax[5]}"
        }
        taxList.append(taxtype)
    return taxList


def taxSummary(docno):
    summary = {}
    sql = ''' select h.netamount, h.taxamount, h.grossamount, COUNT(b.invnumber) from _cplURAInv h inner 
    join _cplUraInvLines b on h.docno = b.invnumber where h.docno = ? AND b.itemprice > 0
    group by h.netamount, h.taxamount, h.grossamount '''
    values = (docno)
    cursor.execute(sql, values)
    for row in cursor:
        summary = {
            "netAmount": f"{row[0]}",
            "taxAmount": f"{row[1]}",
            "grossAmount": f"{row[2]}",
            "itemCount": f"{row[3]}",
            "modeCode": "0",
            "remarks": "",
            "qrCode": ""
        }
        # summaryTax.append(summary)
    return summary
# -------------------------------------------------

# Function that returns a list of all the goods asscociated in an invoice


def getGoods(docno):
    invoiceGoods = []
    no = 0
    sql = '''SELECT [orderlineid],[orderid],[itemname],[itemcode],[itemqty],[itemuom],[itemprice],[linetotprice]
             ,[taxrate],[discount],[discounttaxrate],[invnumber],[discountflag],[deemedflag]
             ,[exciseflag],[categoryid] ,[categoryname],[goodscategoryid],[goodscategoryname]
             ,[taxamount],ROW_NUMBER() over(order by orderlineid)-1 as row
             FROM [dbo].[_cplUraInvLines]
             where invnumber = ?  AND itemprice >0 '''
    values = (docno)
    cursor.execute(sql, values)
    for items in cursor:
        invoiceitems = {
            "item": f"{items[2]}",
            "itemCode": f"{items[3]}",
            "qty": f"{items[4]}",
            "unitOfMeasure": f"{items[5]}",
            "unitPrice": f"{items[6]}",
            "total": f"{items[7]}",
            "taxRate": f"{items[8]}",
            "tax": f"{items[19]}",
            "discountTotal": "",
            "discountTaxRate": "",
            "orderNumber": f"{items[20]}",
            "discountFlag": f"{items[12]}",
            "deemedFlag": f"{items[13]}",
            "exciseFlag": f"{items[14]}",
            "categoryId": "",
            "categoryName": "",
            "goodsCategoryId": f"{items[17]}",
            "goodsCategoryName": "",
            "exciseRate": "",
            "exciseRule": "",
            "exciseTax": "",
            "pack": "",
            "stick": "",
            "exciseUnit": "",
            "exciseCurrency": "",
            "exciseRateName": ""
        }
        invoiceGoods.append(invoiceitems)
        no = no+1
    return invoiceGoods
# --------------------------------------------------------

# MAIN FUNCTION THAT SENDS THE ENTIRE JSON AND MAKES THE NECCESARY CHANGES TO THE DB BASED ON RESPONSES


def sendInvoice():
    docnoList = []
    # executes the stored procedure for the _cplUraInv and _cplUraInvLines tables
    cursor.execute('EXEC invoiceURA')
    print('store procedure done')
    conn.commit()
    # used to create the timestamp for the invoices
    nowdt = datetime.datetime.now()
    dt_plus_2 = nowdt + datetime.timedelta(minutes=2)
    dt = dt_plus_2.strftime("%Y-%m-%d %H:%M:%S")
    # select query for the invoices that are pending
    cursor.execute(
        "Select top 50 * From _cplurainv Where sentstatus ='PENDING' and docname = 'inv'")
    # placing the results in a list
    for row in cursor:
        docnoList.append(row)

    # iterates the loop to find and pupulate the json as needed
    for invoice in docnoList:
        decodedjson = {"sellerDetails": {
            "tin": f"{tin}",
            "ninBrn": "",
            "legalName": f"{legalname}",
            "businessName": f"{legalname}",
            "address": f"{address}",
            "mobilePhone": f"{mobilephone}",
            "linePhone": f"{mobilephone}",
            "emailAddress": f"{email}",
            "placeOfBusiness": f"{placeofbusiness}",
            "referenceNo": f"{invoice[18]}"
        },
            "basicInformation": {
            "invoiceNo": "",
            "antifakeCode": "",
            "deviceNo": f"{deviceno}",
            "issuedDate": "{}".format(dt),
            "operator": "Admin",
            "currency": "UGX",
            "oriInvoiceId": "",
            "invoiceType": "1",
            "invoiceKind": "1",
            "dataSource": "106",
            "invoiceIndustryCode": "101",
            "isBatch": "0"
        },
            "buyerDetails": {
            "buyerTin": f"{invoice[1]}",
            "buyerNinBrn": "",
            "buyerPassportNum": "",
            "buyerLegalName": f"{invoice[4]}",
            "buyerBusinessName":  f"{invoice[4]}",
            "buyerAddress": "",
            "buyerEmail": "",
            "buyerMobilePhone": "",
            "buyerLinePhone":  "",
            "buyerPlaceOfBusi": "",
            "buyerType": f"{invoice[11]}",
            "buyerCitizenship": "",
            "buyerSector": "",
            "buyerReferenceNo": ""
        },
            "goodsDetails": getGoods(invoice[18]),
            "taxDetails": TaxTypes(invoice[18]),
            "summary": taxSummary(invoice[18]),
            "payWay": [{
                "paymentMode": "101",
                "paymentAmount": f"{invoice[23]}",
                "orderNumber": "a"
            }],
            "extend": {
            "reason": "",
            "reasonCode": ""
        },


        }
        # the json gets encoded through a base64
        enocdedInvoices = base64.urlsafe_b64encode(
            json.dumps(decodedjson).encode()).decode()

        # creation of the payload with the content
        payload = "{\r\n    \"data\": {\r\n \"content\": \"%s\",\r\n \"signature\": \"\",\r\n  \"dataDescription\": {\r\n  \"codeType\": \"0\",\r\n \"encryptCode\": \"0\",\r\n                     \"zipCode\": \"0\"\r\n        }\r\n    },\r\n    \"globalInfo\": {\r\n        \"appId\": \"AP04\",\r\n        \"version\": \"1.1.20191201\",\r\n        \"dataExchangeId\": \"%s\",\r\n        \"interfaceCode\": \"T109\",\r\n        \"requestCode\": \"TP\",\r\n        \"requestTime\": \"%s\",\r\n        \"responseCode\": \"TA\",\r\n        \"userName\": \"admin\",\r\n        \"deviceMAC\": \"%s\",\r\n        \"deviceNo\": \"%s\",\r\n        \"tin\": \"%s\",\r\n        \"brn\": \"\",\r\n        \"taxpayerID\": \"%s\",\r\n        \"longitude\": \"%s\",\r\n        \"latitude\": \"%s\"\r\n    },\r\n    \"returnStateInfo\": {\r\n        \"returnCode\": \"\",\r\n        \"returnMessage\": \"\"\r\n    }\r\n}" % (
            enocdedInvoices, uuid, dt, mac, deviceno, tin, tin, longitude, latitude)

        headers = {
            'Content-Type': 'application/json'
        }

        logURA('----------------------------------------------------------------------------------------------------------------')
        logURA(
            f'-------------------------------------------{invoice[18]}---------------------------------------------------------------------')
        logPayload(payload)

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
            elif encryptionType == '0':
                content = invoiceres['data']['content']
                content_decoded = base64.b64decode(content.encode('utf-8'))
                content_decoded = json.loads(content_decoded.decode('utf-8'))
            qrCode = content_decoded['summary']['qrCode']
            invoiceID = content_decoded['basicInformation']['invoiceId']
            invoiceNo = content_decoded['basicInformation']['invoiceNo']
            antifakeCode = content_decoded['basicInformation']['antifakeCode']
            responseStatus = invoiceres['returnStateInfo']['returnMessage']
            docno = invoice[18]
            # insert the uraid and urainvoice no in to the Invnum table
            sql3 = '''update invnum set ucIDSOrdUraInvoiceID = ?, ucIDSOrdUraInvoiceNo = ?, ucIDSOrdUraStatus=?, ucIDSOrdverificationcode=? where invnumber = ?'''
            values3 = (invoiceID, invoiceNo, responseStatus, antifakeCode,
                       invoice[18])
            cursor.execute(sql3, values3)
            conn.commit()

            sql1 = '''UPDATE _cplURAInv SET sentstatus = 'SUCCESS', sentlastupdate= ?, qrcode = ? WHERE docno = ? '''
            values1 = (dt, qrCode, invoice[18])
            cursor.execute(sql1, values1)

            conn.commit()
            sql = '''update invnum set ucIDSOrdqrcode = ? where invnumber = ? '''
            values = (fr"{qrcodefolder}\{docno}.png", invoice[18])
            cursor.execute(sql, values)
            conn.commit()
            logSummary(
                f"{invoice[18]} was a success and the URA inv no is : {invoiceNo} and InvoiceID = {invoiceID}")
            print(
                f"{invoice[18]} was a success and the URA inv no is : {invoiceNo} and InvoiceID = {invoiceID}")
            qr = pyqrcode.create(qrCode)
            qr.png(
                fr"{qrcodefolder}\{invoice[18]}.png", scale=2, quiet_zone=1)
        elif invoiceres['returnStateInfo']['returnCode'] == '2253':
            sql1 = '''UPDATE _cplURAInv SET sentstatus = 'SUCCESS', sentlastupdate= ?, qrcode ='' WHERE docno = ? '''
            values1 = (dt, invoice[18])
            cursor.execute(sql1, values1)
            conn.commit()
            print(f"{invoice[18]} was sent earlier")
        else:

            # THIS SECTION DEALS WITH IN THE EVENT OF A FAILURE OF IN THE SENDING OF THE JSON
            responseStatus = invoiceres['returnStateInfo']['returnMessage']
            sql3 = '''update invnum set ucIDSOrdUraStatus=? where invnumber = ?'''
            values3 = (responseStatus, invoice[18])
            cursor.execute(sql3, values3)
            conn.commit()
            sql1 = '''UPDATE _cplURAInv SET sentstatus = 'PENDING', sentlastupdate= ? WHERE docno = ? '''
            values1 = (dt, invoice[18])
            cursor.execute(sql1, values1)
            conn.commit()
            logSummary(
                f"{invoice[18]} was not success due to {responseStatus}")
            print(f"{invoice[18]} was not success due to {responseStatus}")
        logURA("\n\n")
        time.sleep(5)


sendInvoice()
# ENSURES THAT THE MAIN FUNCTION IS OPERATING AFTER EVERY 3 MINUTES
schedule.every(1).minutes.do(sendInvoice)
while True:
    schedule.run_pending()
    time.sleep(1)
