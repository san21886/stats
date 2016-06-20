import os
import sys
import smtplib
import email.utils
from email.mime.text import MIMEText
import getpass
from prettytable import PrettyTable
import HTML
#sudo pip install prettytable

class MyMail(object):
    def __init__(self,args_dict):
        print("Initializing "+self.__class__.__name__)
        if not args_dict.get("smtp_server") or not args_dict.get("smtp_port"):
            raise Exception("Could not find smtp_server/smtp_port in args_dict.")
        self.smtp_server=smtplib.SMTP(args_dict.get("smtp_server"),args_dict.get("smtp_port") )
        if args_dict.get("debug"):
            self.smtp_server.set_debuglevel(True)
        self.args_dict=args_dict
        

    def sendmail(self,fromaddr,toaddr,subject,message,mimetext="plain"):
        msg=MIMEText(message,mimetext)
        msg['From'] = fromaddr
        msg['To'] = toaddr
        msg['Subject'] = subject
        print("Sending mail...")
        self.smtp_server.starttls()
        if self.args_dict.get("username") and self.args_dict.get("password"):
	    self.smtp_server.login(self.args_dict.get("username"),self.args_dict.get("password"))
        self.smtp_server.sendmail(fromaddr,toaddr.split(","), str(msg))

if __name__=="__main__":
    print("Running "+sys.argv[0])
    args_dict={"smtp_server":os.getenv("SMTP_HOST","smtp.gmail.com"),
    "smtp_port":os.getenv("SMTP_PORT","587"),
    "username":os.getenv("MAIL_FROM","mailer@datarpm.com"),
    "password":os.getenv("SMTP_PASS","drpmm41l3r#")}
    mail=MyMail(args_dict)
    #x = PrettyTable()
    #x.set_field_names(["h1", "h2", "h3", "h4"])
    #x.field_names = ["City name", "Area", "Population", "Annual Rainfall"]
    #x.add_row(["Adelaide",1295, 1158259, 600.5])
    #x.add_row(["Brisbane",5905, 1857594, 1146.4])
    #x.add_row(["Darwin", 112, 120900, 1714.7])
    
    table_data = [
        ['Last name',   'First name',   'Age'],
        ['Smith',       'John',         30],
        ['Carpenter',   'Jack',         47],
        ['Johnson',     'Paul',         62],
    ]
    x = "<html>"+str(HTML.table(table_data))+"</html>"
    print x
    mail.sendmail("santosh@datarpm.com","santosh@datarpm.com","PyMail",str(x))
