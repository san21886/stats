import sys
import os
import json
import copy
import datarpm_meta_query
import hadoop_stats
import index_size
import mymail
from prettytable import PrettyTable
import HTML

class ClusterStats(object):

    def __init__(self, args_dict):
        print("Initializing "+self.__class__.__name__)
        self.drpm_meta=datarpm_meta_query.DrpmMeta(args_dict)
        self.hadoop_stats=hadoop_stats.HadoopAPI(args_dict)
        self.index_stats=index_size.ESStat(args_dict)
        self.hadoop_data_dirs=args_dict.get("hadoop_dirs").split(",")

    def get_hadoop_stats(self):
        data_size=self.hadoop_stats.get_hdfs_data_size(self.hadoop_data_dirs)
        replication=self.hadoop_stats.get_hdfs_replication()
        for key,value in data_size.iteritems():
            data_size[key]=value/((replication+1)*1024*1024*1024.0)
        return data_size

    def get_index_stats(self):
        size=self.index_stats.get_es_data_size()
        indexed_record_count=self.drpm_meta.get_count_indexed_record()
        return {"es_index_size_without_replication":size,"es_index_record_count":indexed_record_count}

    def get_user_stats(self):
        non_drpm_users=self.drpm_meta.get_non_datarpm_users()
        return {"non_drpm_users":non_drpm_users}

    def get_cluster_stats_html(self):
        headers=["Hadoop data size(G)","Index data size(G)","Indexed record count","Number of users"]
        hadoop_size=self.get_hadoop_stats()
        es_size=self.get_index_stats()
        users=self.get_user_stats().get("non_drpm_users")
        table1_data=[headers,[hadoop_size.get("total"),es_size.get("es_index_size_without_replication"),es_size.get("es_index_record_count"),users.count()]]
        table1="<html>CLUSTER SUMMARY:<br>"+str(HTML.table(table1_data))+"<br></html>"
        row=list()
        path_headers=copy.deepcopy(self.hadoop_data_dirs)
        path_headers.append("total")
        for path in path_headers:
            row.append(hadoop_size.get(path))
        table2="<html>HADOOP SUMMARY(G):<br>"+str(HTML.table([path_headers,row]))+"<br></html>"
        return str(table1)+str(table2)

    def get_cluster_stats_text(self):
        headers=["Hadoop data size(G)","Index data size(G)","Indexed record count","Number of users"]
        pt1 = PrettyTable()
        pt1.field_names = headers
        hadoop_size=self.get_hadoop_stats()
        es_size=self.get_index_stats()
        users=self.get_user_stats().get("non_drpm_users")
        table1_data=[hadoop_size.get("total"),es_size.get("es_index_size_without_replication"),es_size.get("es_index_record_count"),users.count()]
        pt1.add_row(table1_data)
        table2_data=list()
        path_headers=copy.deepcopy(self.hadoop_data_dirs)
        path_headers.append("total")
        pt2 = PrettyTable()
        pt2.field_names = path_headers
        for path in path_headers:
            table2_data.append(hadoop_size.get(path))
        pt2.add_row(table2_data)
        return (pt1,pt2)

    def mail_cluster_stats(self):
        msg=self.get_cluster_stats_html()
        mail_dict={"smtp_server":os.getenv("SMTP_HOST","smtp.gmail.com"),
        "smtp_port":os.getenv("SMTP_PORT","587"),
        "username":os.getenv("MAIL_FROM","mailer@datarpm.com"),
        "password":os.getenv("SMTP_PASS","drpmm41l3r#")}
        mail=mymail.MyMail(mail_dict)
        subject=os.getenv("CLUSTER_ID","Unkown cluster")+" usage summary"
        mail.sendmail(os.getenv("MAIL_FROM","santosh@datarpm.com"),os.getenv("NOTIFICATION_EMAIL","santosh@datarpm.com"),subject,msg,"html")

if __name__=="__main__":
    print("Running "+sys.argv[0])
    args_dict={"mongo_host":os.getenv("METADB_NODE","127.0.0.1"),"mongo_port":"27017","mongo_app_db":"datarpm_app_prod","mongo_loader_job_db":"loader-job",
    "hadoop_nn_host":os.getenv("HADOOP_NAME_NODE","127.0.0.1"),"hadoop_nn_rest_port":"50070","hadoop_dirs":"/hbase,/user/hive/,/user/datarpm/dataframe,/user/datarpm/data/dataframe",
    "es_hosts":os.getenv("SEARCH_NODES","127.0.0.1"),"es_data_dirs":"/ebs1/common/data/es/path.data/","es_replication":1}
    obj=ClusterStats(args_dict)
    print(str(obj.get_cluster_stats_text()[0])+"\n"+str(obj.get_cluster_stats_text()[1]))
    if len(sys.argv)>1 and sys.argv[1]=="sendmail":
        obj.mail_cluster_stats()
