
# coding: utf-8

# In[379]:


# Import necessary libraries

import pymysql as db
import pandas as pd
import numpy as np
import time
import smtplib
from sys import argv,exit


# In[380]:


# Global variables 

dbHost = "mysql.seil.cse.iitb.ac.in"
dbReader = "reader"
dbWriter = "writer"
dbPassword = "reader"
dbDatabase = "seil_sensor_data"
dataTable = "dht_7"
# sensorID = "temp_k_seil"
sensorID = "temp_k_204"
activeSensors = []
deadSensors = []

#  Sending mails
user = "seil"
pwd = "seilers"
subject = ""
body = ""
header = "Node ID : Last Logged TS\n"
to = ["shaunaksmanurkar@gmail.com",
        "a.anshul215@gmail.com"]

inactiveSensorsPresent = False


# In[381]:


def init_mail():
    smtpserver = smtplib.SMTP("smtp.cse.iitb.ac.in",25)
    smtpserver.ehlo()
    smtpserver.starttls()
    smtpserver.ehlo()
    smtpserver.esmtp_features['auth'] = 'LOGIN DIGEST-MD5 PLAIN'

    return smtpserver


# In[382]:


def close_mail_connection(smtpserver):
    smtpserver.close()


# In[383]:


def send_alert_mail(to,body,subject):
    global header
    
    smtpserver = init_mail()
    smtpserver.login(user,pwd)
    msg = header + '\n' + body + '\n\n'
    for i in range(0, len(to)):
        header = "To:" + to[i] + '\n' + 'From: ' + user +'@cse.iitb.ac.in\n' + 'Subject:'+subject+'\n'

        smtpserver.sendmail(user,to[i],msg)
    close_mail_connection(smtpserver)


# In[384]:


# Function connects to the database

def create_connection(user, pswd):
    con = db.connect(dbHost, user, pswd, dbDatabase)
    cursor = con.cursor()
    
    return con, cursor


# In[385]:


# Returns the number of current active nodes

def get_nodes():
    con, cursor = create_connection(dbReader, dbPassword)
    
    sql = ("SELECT distinct(id) FROM seil_sensor_data.dht_7 where sensor_id = '"+ sensorID +
           "' and TS > 1531872000 order by id")
    
    numberOfNodes = []
    
    try:
        cursor.execute(sql)
        
        results = cursor.fetchall()
        for row in results:
            numberOfNodes.append(row[0])
#         print("Fetched data")
        
    except:
        print("Error fetching data..!!")
        
    con.close()
    
    return numberOfNodes


# In[386]:


def get_time():
    return int(time.time())


# In[387]:


def get_active_nodes():
    con, cursor = create_connection(dbReader, dbPassword)
    
    currentTS = get_time()
    previousTS = str(currentTS - 600) # This will check if the sensor has some any data in last 10 minutes
#     print("Previous TS is %s" %type(previousTS))
    
#     sql = ("SELECT distinct(id) FROM seil_sensor_data.dht_7 where sensor_id = '"+ sensorID +"' and TS > "+ previousTS + " order by id")

    totalNodes = get_nodes()
    totalActiveNodes = []
    sensorData = []
    
    for i in range(1, len(totalNodes) + 1):
        sql = ("SELECT * FROM seil_sensor_data.dht_7 where sensor_id = 'temp_k_204' and id='"+ str(i)+"' order by TS DESC limit 1;")
    
        try:
            cursor.execute(sql)

            results = cursor.fetchall()
            for row in results:
                sensorData.append(row)
#                 totalActiveNodes.append(row[0])
#             print("Fetched data")

        except Exception as e:
            continue
#             print("Error fetching data..!!")
#             print(str(e))
    
#     return totalActiveNodes
    con.close()
    return totalNodes, list(sensorData)

get_active_nodes()


# In[388]:


if __name__ == "__main__":
    global body
    global subject
    
    totalNodes, tempList = get_active_nodes()
    nodeDF = pd.DataFrame(tempList, columns=['sensor_id', 'TS_RECV', 'TS', 'ID', 'TEMP', 'HUMID', 'BATTERY_VOLT'])
    
    activeNodes = len(nodeDF.index)
    
    currentTime = get_time()
    
    for i in range(0, activeNodes):
        if nodeDF['TS'][i] - currentTime > 900:
#             print("Node %d is inactive" %nodeDF['ID'][i])
            body = body + str(nodeDF['ID'][i]) + " : " + str(time.ctime(nodeDF['TS'][i])) + "\n"
            inactiveSensorsPresent = True
        else:
            continue
#             print("Node %d is active" %nodeDF['ID'][i])
            
    if(inactiveSensorsPresent):
        subject = "Sensor Data Alert : 204"
        send_alert_mail(to, body, subject)
        body = ""
        inactiveSensorsPresent = False

