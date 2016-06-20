import sys
import mongo_connect
import re

class DrpmMeta(object):

    def __init__(self, args_dict):
	self.mongo_client=mongo_connect.MongoConnect(args_dict)
        self.app_db=self.mongo_client.get_mongo_db(args_dict.get("mongo_app_db"))
        self.loader_job_db=self.mongo_client.get_mongo_db(args_dict.get("mongo_loader_job_db"))

    def get_non_datarpm_users(self):
        return self.mongo_client.find_with_query(self.app_db["User"],{"email":{'$not':re.compile('.*datarpm.*')}})

    def get_count_indexed_record(self):
        result=self.mongo_client.find_with_query(self.loader_job_db["PartitionLoaderWorkerJob"],{"deleteFlag" : False})
        record_count=0
        for doc in result:
            record_count+=doc.get("loaderQueryState").get("totalRecordsLoaded")
        return record_count
if __name__=="__main__":
    print("Running "+sys.argv[0])
    args_dict={"mongo_host":"127.0.0.1","mongo_port":"27017","mongo_app_db":"datarpm_app_prod","mongo_loader_job_db":"loader-job"}
    drpm_meta=DrpmMeta(args_dict)
    non_drpm_users=drpm_meta.get_non_datarpm_users()
    print("Non drpm users count:"+str(non_drpm_users.count()))
    indexed_record_count=drpm_meta.get_count_indexed_record()
    print("Indexed record count:"+str(indexed_record_count))
