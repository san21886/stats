#!/usr/bin/python
#https://hadoop.apache.org/docs/r1.2.1/webhdfs.html#CREATE

import sys
import rest_lib
import json
#import httplib2
#sudo pip install httplib2

class HadoopAPI(object):

    def __init__(self,args_hash={}):
        request_protocol="http://"
        if args_hash.get("secure"):
            request_protocol="https://"
        if not args_hash.get("hadoop_nn_host") or not args_hash.get("hadoop_nn_rest_port"):
            raise Exception("Could not find hadoop_nn_host/hadoop_nn_rest_port for the API.")
        self.base_url=request_protocol+args_hash["hadoop_nn_host"]+":"+args_hash["hadoop_nn_rest_port"]+"/webhdfs/v1"
        self.rest_client=rest_lib.RestClient(args_hash)

    def get_contentsummary(self, path):
        url=self.base_url+path+"?op=GETCONTENTSUMMARY"
        ret_tuple=self.rest_client.make_request(url)
	print(ret_tuple)
        if ret_tuple[0].status==200:
            return json.loads(ret_tuple[1])
        else:
            print("Call to URL:"+url+" failed with error code:"+str(ret_tuple[0].status))
            return {"resp":ret_tuple[0]}

    def get_hdfs_data_size(self, paths):
        data_size=0L
        size_dict=dict()
        for path in paths:
            resp_json=self.get_contentsummary(path)
            if resp_json.get("ContentSummary"):
                size_dict[path]=resp_json.get("ContentSummary").get("length")
                data_size=data_size+size_dict.get(path)
            else:
                size_dict[path]=0
        size_dict["total"]=data_size
        return size_dict

    def get_hdfs_replication(self):
        url=self.base_url+"/?op=GETFILESTATUS"
        ret_tuple=self.rest_client.make_request(url)
        if ret_tuple[0].status==200:
            return json.loads(ret_tuple[1]).get("FileStatus").get("replication")
        else:
            print("Call to URL:"+url+" failed with error code:"+str(ret_tuple[0].status))
            return json.dumps({"resp":ret_tuple[0]})

if __name__=="__main__":
    print("Running "+sys.argv[0])
    args_hash={"secure":False,"hadoop_nn_rest_port":"50070","hadoop_nn_host":"127.0.0.1"}
    if len(sys.argv)==3:
        args_hash={"secure":False,"hadoop_nn_rest_port":sys.argv[2],"hadoop_nn_host":sys.argv[1]}
    obj=HadoopAPI(args_hash)
    data_size=obj.get_hdfs_data_size(("/hbase","/user/hive/","/user/datarpm/dataframe","/user/datarpm/data/dataframe"))
    replication=obj.get_hdfs_replication()
    for key,value in data_size.iteritems():
        data_size[key]=value/((replication+1)*1024*1024*1024.0)
    print(data_size)
