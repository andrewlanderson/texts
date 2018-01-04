#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 30 21:04:56 2017

@author: andy
"""

from bs4 import BeautifulSoup
from datetime import datetime
from urllib import parse
from elasticsearch import Elasticsearch
from datetime import datetime


with open("/Users/andy/Desktop/Texts/sms-20171129230423-0.xml", "r") as f:
    fileA = f.read()
with open("/Users/andy/Desktop/Texts/sms-20171129230423-1.xml", "r") as f:
    fileB = f.read()
with open("/Users/andy/Desktop/Texts/SignalPlaintextBackup.xml", "r") as f:
    fileC = f.read()
    
    
soupA = BeautifulSoup(fileA)
soupB = BeautifulSoup(fileB)
soupC = BeautifulSoup(fileC)

messagesA = soupA("message")
messagesB = soupB("message")
messagesC = soupC("sms")

#total = len(messagesA)+len(messagesB)+len(messagesC) #8741

listA = []
for message in messagesA:
    date = datetime.fromtimestamp(int(message.find("date").text[:-3]))
    num = re.sub('[\+\(\)\-]', "", message.find('address').text)
    body = parse.unquote_plus(message.text.split("\n")[2].strip())
    direction = "incoming" if message.find("type").text=="1" else "outgoing"
    listA.append({"phoneNumber":num, "date":date, "direction":direction, "body":body })

listB = []
for message in messagesB:
    date = datetime.fromtimestamp(int(message.find("date").text[:-3]))
    num = re.sub('[\+\(\)\-]', "", message.find('address').text)
    body = parse.unquote_plus(message.text.split("\n")[2].strip())
    direction = "incoming" if message.find("type").text=="1" else "outgoing"
    listB.append({"phoneNumber":num, "date":date, "direction":direction, "body":body })

#type 1->incoming, 2->outgoing
listC = []
for message in messagesC:
    direction = "incoming" if message("type")=="1" else "outgoing"
    date = datetime.fromtimestamp(int(message['date'][:-3]))
    listC.append({"phoneNumber":re.sub('[\+\(\)\-]', "", message['address']), "body":message["body"], 
                  "contactName":message["contact_name"], "date":date,
                  "direction":direction})
    
es = Elasticsearch()
messageList = listA + listB + listC

es.indices.create(index='texts', ignore=400)
for message in messageList:
    es.index(index='texts', doc_type='people', body=message)
    
    
    