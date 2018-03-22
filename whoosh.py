#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 13 21:27:42 2018

@author: andy
"""

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
q = qp.parse("912255*")
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
