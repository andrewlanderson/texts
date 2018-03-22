#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 30 21:04:56 2017

@author: andy
"""

#from bs4 import BeautifulSoup
#from urllib import parse
#from elasticsearch import Elasticsearch
#from datetime import datetime
#import re


##get data

##get data
#with open("/Users/andy/Desktop/Texts/sms-20171129230423-0.xml", "r") as f:
#    fileA = f.read()
#with open("/Users/andy/Desktop/Texts/sms-20171129230423-1.xml", "r") as f:
#    fileB = f.read()
#with open("/Users/andy/Desktop/Texts/SignalPlaintextBackup.xml", "r") as f:
#    fileC = f.read()
#with open("/Users/andy/Desktop/Texts/SignalPlaintextBackupFeb2018.xml", "r") as f:
#    fileD = f.read()    
## clean data
#soupA = BeautifulSoup(fileA)
#soupB = BeautifulSoup(fileB)
#soupC = BeautifulSoup(fileC)
#soupD = BeautifulSoup(fileD)
#
#
#
#messagesA = soupA("message")
#messagesB = soupB("message")
#messagesC = soupC("sms")
#messagesD = soupD("sms")
#
##total = len(messagesA)+len(messagesB)+len(messagesC) #8741
#
#smsList = []
#for messages in [messagesA, messagesB]:
#    for message in messages:
#        date = datetime.fromtimestamp(int(message.find("date").text[:-3]))
#        num = re.sub('[\+\(\)\-]', "", message.find('address').text)
#        body = parse.unquote_plus(message.text.split("\n")[2].strip())
#        direction = "incoming" if message.find("type").text=="1" else "outgoing"
#        smsList.append({"phoneNumber":num, "date":date, "direction":direction, "body":body })
#
##type 1->incoming, 2->outgoing
#signalList = []
#for messages in [messagesC, messagesD]:  
#    for message in messages:
#        direction = "incoming" if message("type")=="1" else "outgoing"
#        date = datetime.fromtimestamp(int(message['date'][:-3]))
#        signalList.append({"phoneNumber":re.sub('[\+\(\)\-]', "", message['address']), "body":message["body"], 
#                      "contactName":message["contact_name"], "date":date,
#                      "direction":direction})
##    
#messageList = smsList + signalList

##index in Elasticsearch

#myNum = my number as a string

#es = Elasticsearch()
#es.indices.create(index='texts', ignore=400)
#for message in messageList:
#    es.index(index='texts', doc_type='people', body=message)


#playing with some different formatting
#df = pd.DataFrame(messageList)
#
#def toFrom(row):
#    if row['direction']=="incoming":
#        to_ = myNum
#    elif row["direction"]=="outgoing":
#        to_ = row['phoneNumber']
#    return to_
#
#def fromTo(row):
#    if row['direction']=="incoming":
#        from_ = row['phoneNumber']
#    elif row["direction"]=="outgoing":
#        from_ = myNum
#    return from_    
#
#df["To"] = df.apply(toFrom, axis=1)
#df["From"] = df.apply(fromTo, axis=1)
#
#df = df.drop(['body', 'contactName', "direction", 'phoneNumber'], 1)
#df["myNum"] = myNum

################################
#whoosh index 
################################
import os, os.path
import whoosh.index as index
from whoosh.index import create_in
from whoosh.fields import *
from whoosh.analysis import StemmingAnalyzer
from datetime import datetime
import re


##create schema first time index is initialized
#schema = Schema(phoneNumber=TEXT(stored=True),
#                date=DATETIME(stored=True),
#                direction=KEYWORD(stored=True),
#                body=TEXT(analyzer=StemmingAnalyzer(), stored=True))
##initiate index
indexdir = "//Users/andy/data/texts/whooshIndexDir"
#if not os.path.exists(indexdir):
#    os.mkdir(indexdir)
#ix = index.create_in(indexdir, schema, indexname="texts")

ix = index.open_dir(indexdir, indexname="texts")

#initiate writer from index object
#writer = ix.writer(limitmb=1024)
#
#for message in messageList:
#    writer.add_document(phoneNumber=message["phoneNumber"], date=message["date"], 
#                        direction=message["direction"], body=message["body"])
#writer.commit()

#get field values
searcher = ix.searcher()
list(searcher.lexicon("phoneNumber"))

#query setup
from whoosh.qparser import QueryParser
from whoosh.query import Term
#from whoosh.query import Wildcard 


qp = QueryParser("phoneNumber", schema=ix.schema)
q = qp.parse("*")
#searcher = ix.searcher()


#use with statment to auto close
with ix.searcher() as s:
#    results = s.search(q)
    results = s.search(q, terms=True) #tells you which terms match
   
    # Was this results object created with terms=True?
if results.has_matched_terms():
    # What terms matched in the results?
    print(results.matched_terms())

    # What terms matched in each hit?
    for hit in results:
        print(hit.matched_terms())
##limit number of returns (default=10)
#results = s.search(q, limit=20)
##return results for page five with length of 20 per page
#results = s.search_page(q, 5, pagelen=20) 

#number of documents
numDocs = searcher.doc_count_all()

#sometimes convenient not to automatically close it using with statement
results = ix.searcher().search(q, terms=True)
for r in results:
    print(r)
ix.searcher.close()
