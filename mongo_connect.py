#!/usr/bin/python
#http://ming.readthedocs.io/en/latest/baselevel.html

import sys
import pymongo
from pymongo import MongoClient
import re

class MongoConnect(object):

    def __init__(self, args_hash):
        print("Initializing MongoConnect...")
        if not args_hash.get("mongo_host") or not args_hash.get("mongo_port"):
            raise Exception("Could not find mongo_host/mongo_port information in the agrument hash.")
        self.mongo_client=MongoClient(args_hash.get("mongo_host")+':'+args_hash.get("mongo_port"))

    def get_mongo_db(self, db_name):
        return self.mongo_client[db_name]

    def get_mongo_collection(self, db_name, collection_name):
        db=self.get_mongo_db(db_name)
        return  db[collection_name]

    def find_with_query(self, mongo_collection, query):
        return mongo_collection.find(query)


if __name__=="__main__":
    obj=MongoConnect({"host":"127.0.0.1","port":"27017"})
    collection=obj.get_mongo_collection("datarpm_app_prod","User")
    datarpm_users=obj.find_with_pattern(collection,{"email":{'$regex':re.compile('.*datarpm.*')}})
    non_datarpm_users=obj.find_with_pattern(collection,{"email":{'$not':re.compile('.*datarpm.*')}})
    print("DataRPM Users:"+str(datarpm_users.count()))
    print("Non DataRPM Users:"+str(non_datarpm_users.count()))
