import sys
import unix_lib
import re

class ESStat(object):
    def __init__(self, args_dict):
        print("Initializing ESStat....")
        self.replication=args_dict.get("es_replication")
        self.hosts=args_dict.get("es_hosts")
        self.dirs=args_dict.get("es_data_dirs")
        self.unix_lib=unix_lib.UnixLib(args_dict)

    def get_es_data_size(self):
        data_size=0
        for host in self.hosts.split(","):
            for path in self.dirs.split(","):
                dir_size_info=self.unix_lib.execute_shell_cmd(["ssh",host,"du","-sh",path])
                dir_size=float(re.sub(r'G|M',"",dir_size_info[0].rstrip().split("\t")[0]))/self.replication
                data_size+=dir_size
        return data_size
        

if __name__=="__main__":
    print("Running "+sys.argv[0])
    obj=ESStat({"es_replication":1,"es_hosts":"127.0.0.1","es_data_dirs":"/ebs1/common/data/es/path.data/"})
    size=obj.get_es_data_size()
    print(size)
