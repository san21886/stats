import sys
import httplib2
import json
#https://github.com/httplib2/httplib2
class RestClient(object):

    def __init__(self,args_hash):
        print("Initializing RestClient....")
        if args_hash.get("cache_dir"):
            self.rest_client=httplib2.Http(args_hash.get("cache_dir"))
        else:
            self.rest_client=httplib2.Http()
        self.authenticate(args_hash.get("credential"))

    def make_request(self, url):
        (resp, content) = self.rest_client.request(url, "GET")
        return (resp, content)

    def authenticate(self, credential_hash):
        if credential_hash:
            self.rest_client.add_credentials(credential_hash.get("name"), credential_hash.get("password"))

if __file__==sys.argv[0]:
    print(RestClient({"cache_dir":"urllib2cache"}).make_request("http://datarpm.com"))
