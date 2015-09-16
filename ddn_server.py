#! /usr/bin/python
from util import  ddn_role
import command_daemon
import getopt
import sys
__author__ = 'jzheng'

def usage():
    print "usage:ddn_server -r role(super|index)"
if __name__=="__main__":
    """
    startup  server entity here ,and resolve commands from underlay clients
    """
    role=None
    args=sys.argv[1:]
    opt,arg=getopt.getopt(args,"r:")
    for tup in opt:
        op,ar=tup
        if op == "-r":
            if ar=="super":
                role=ddn_role.role_super
            elif ar=="index":
                role=ddn_role.role_index
            else:
                role=None
    if not role:
        usage()
        sys.exit(1)
    command_daemon.start_daemon(node_role=role)

