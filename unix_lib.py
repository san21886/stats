import subprocess
import sys

class UnixLib(object):

    def __init__(self,args_has):
        print("Initializing UnixLib...")

    def execute_shell_cmd(self,cmd_list):
        p=subprocess.Popen(cmd_list, stdout=subprocess.PIPE)
        output, stderr=p.communicate()
        return (output,stderr)


#if __file__==sys.argv[0]:
if __name__=="__main__":
    print(UnixLib(dict()).execute_shell_cmd(["du", "-sh"]))
    cmd_out=UnixLib(dict()).execute_shell_cmd(["ssh", "172.31.16.177", "du", "-sh", "/ebs1/common/data/es/path.data/"])
    print(cmd_out[0].strip())
