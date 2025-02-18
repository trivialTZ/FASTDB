import sys
import json
import time
import io
import os
import datetime
from psycopg2.extras import execute_values
from psycopg2 import sql
import psycopg2
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId
import pprint
import urllib.parse
import random

# Connect to MongoDB

username = urllib.parse.quote_plus('alert_writer')
password = urllib.parse.quote_plus(os.environ['MONGODB_PASSWORD'])

client = MongoClient("mongodb://%s:%s@fastdb-mongodb:27017/alerts" % (username,password))    
db = client.alerts
collection = db.messages

alerts=collection.find({"msg.brokerName":"ALeRCE"})

# Connect to the fake PPDB

# Get password

secret = os.environ['PPDB_READER_PASSWORD']
conn_string = "host='fastdb-ppdb-psql' dbname='ppdb' user='ppdb_reader' password='%s'" % secret.strip()
print("Connecting to database %s" % conn_string)
conn = psycopg2.connect(conn_string)

cursor = conn.cursor()
print("Connected")

# Get list of DiaObject Ids

#diaObjectId=42337336

query = sql.SQL("SELECT {} from {}").format(sql.Identifier('diaObjectId'),sql.Identifier('DiaObject'))
cursor.execute(query)

#query = sql.SQL("SELECT {} from {} where {} = %s").format(sql.Identifier('diaObjectId'),sql.Identifier('DiaObject'),sql.Identifier('diaObjectId'))
#cursor.execute(query,(diaObjectId,))

if cursor.rowcount != 0:
    objects = cursor.fetchall()
    
    for o in objects:

        print('ObjectId = %s' % o[0])
        alert = {}
        # get random alert from MongoDB
        result = collection.aggregate(pipeline=[{"$sample":{ "size":1 }}])
        for  r in result:
            alert['msg'] = r['msg']
            alert['msgoffset'] = r['msgoffset']
            alert['timestamp'] = r['timestamp']
            alert['topic'] = r['topic']

        # Find all the DiaSources that point to that Object

        query = sql.SQL( "SELECT {} FROM {}  where {} = %s").format(sql.Identifier('diaSourceId'),sql.Identifier('DiaSource'),sql.Identifier('diaObjectId'))
        cursor.execute(query,(o[0],))
        count = cursor.rowcount
        print('DiaSources = %s' % count)
        if count > 1:

            # Make rowcount increasing timestamps
            
            start_date = datetime.datetime(2023, random.randint(1,12), random.randint(1,28), random.randint(1,23), random.randint(1,59), random.randint(1,59), 367000)
            
            sources = cursor.fetchall()
            
            start_alertId = alert['msg']['alertId']

            for s in sources:
                
                # Generate fake alert for this DiaSource

                doc = {}
                doc['msg'] = alert['msg']
                doc['msgoffset'] = alert['msgoffset']
            
                doc['msg']['diaSourceId'] = s[0]
 
                new_alertId = start_alertId + random.randrange(1,count)
                start_alertId = new_alertId
                doc['msg']['alertId'] = new_alertId

                delta_d = random.randrange(1,6)
                delta_h = random.randrange(1,23)
                delta_m = random.randrange(1,60)
                delta_s = random.randrange(1,60)

                new_date = start_date + datetime.timedelta(days=delta_d, hours=delta_h, minutes=delta_m, seconds=delta_s)

                
                start_date = new_date

                doc['timestamp'] = new_date
                date_string = new_date.strftime("%Y%m%d")
                topic = alert['topic'] + date_string
                elements = topic.split('_')
                new_topic = '%s_%s_%s_%s' % (elements[0],elements[1],elements[2],date_string)
                doc['topic'] = new_topic

                state = collection.insert_one(doc)                
            
