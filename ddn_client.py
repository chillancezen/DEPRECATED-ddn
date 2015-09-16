#! /usr/bin/python
from util import  ddn_role
from util import  ddn_log
import threading
import client_entity
import  getopt
import sys
import signal
__author__ = 'jzheng'

def usage():
    print "usage:ddn_client \n\t--index_ip ipaddress\n\t--index_port port\n\t" \
          "--upper_ip ipaaddress(gene node only)\n\t--upper_port port(gene node only)\n\t-r(--role) gene|super"
if __name__=="__main__":
    index_ip="127.0.0.1"
    index_port=507
    upper_ip="127.0.0.1"
    upper_port=507
    role=None
    args=sys.argv[1:]
    opt,arg=getopt.getopt(args,'r:',["index_ip=","index_port=","upper_ip=","upper_port=","role="])
    for op,ar in opt:
        if op=="--index_ip":
            index_ip=ar
        elif op=="--index_port":
            index_port=int(ar)
        elif op=="--upper_ip":
            upper_ip=ar
        elif op=="--upper_port":
            upper_port=int(ar)
        elif op in ("-r","--role"):
            if ar == "gene":
                role=ddn_role.role_gene
            elif ar=="super":
                role=ddn_role.role_super
    if not role:
        usage()
        sys.exit(1)
    #demo code
    ce=client_entity.client_entity(index_ip=index_ip,index_port=index_port,upper_port=upper_port,upper_ip=upper_ip,role=role)
    rc=ce.register_node()
    if rc == True:

        fd=None
        if role ==ddn_role.role_super:
            fd=ce.index_fd
        else:
            fd=ce.upper_fd
        ce.start_heart_beat_thread(fd)

        threading._sleep(30)
        ce.unregister_node()
        ce.dispose_node()


    else :
        print "[x] registery fails"
        ce.dispose_node()

    pass