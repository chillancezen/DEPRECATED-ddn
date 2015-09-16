#! /usr/bin/python

import threading
import json
import socket
from util import  ddn_log
from util import ddn_role

__author__ = 'jzheng'

def generate_msg(**kargs):
    return json.dumps(kargs, indent=2)
def resolve_msg(arg):
    try:
        dic=json.loads(arg)
        return dic
    except:
        return {}

class client_entity:
    """
    client entity which represents gene or super node in the delivery network
    the should be instantiated by gene node or super node

    """
    role=None
    def __init__(self,role,upper_ip="127.0.0.1",upper_port=507,index_ip="127.0.0.1",index_port=507):
        """
        the ip and port needed
        :param role:
        :param upper_ip:
        :param upper_port:
        :return:
        """
        self.role=role
        self.upper_ip=upper_ip
        self.upper_port=upper_port
        #self.fd=None
        self.index_ip=index_ip
        self.index_port=index_port
        self.index_fd=None
        self.upper_fd=None
        # initialize some fds
        self.index_fd=self.get_tcp_socket_fd(self.index_ip,self.index_port)
        self.upper_node_id=None #only if this is a gene node
        self.node_id=None #available for all nodes
        self.uuid=None#available for gene node
        """
        if self.role == ddn_role.role_gene:
            self.index_fd=self.get_tcp_socket_fd(self.index_ip,self.index_port)
            self.upper_fd=self.get_tcp_socket_fd(self.upper_ip,self.upper_port)
        elif self.role == ddn_role.role_super:
            self.index_fd=self.get_tcp_socket_fd(self.index_ip,self.index_port)
        """
        pass
    def get_tcp_socket_fd(self,ip,port):
        fd=None
        try:
            fd=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            fd.connect((ip,port))
        except:
            if fd:
                fd.close()
            fd=None
        return fd
        pass
    def send_msg(self,fd,arg):
        try:
            rc=fd.send(arg)
            return True
        except:
            return  False
    def recv_msg(self,fd):
        try:
            msg=fd.recv(1460)
            return  msg
        except:
            return None

    def register_node(self):
        """
        note that if it's super node,then index_fd will be there open
        otherwise it's a gene node,then,upper_fd will be open,if success ,not closed
        :return:
        """
        msg_dic={}
        if self.role == ddn_role.role_super:
            msg_dic["message_type"] = "super_node_register"
            rc=self.send_msg(self.index_fd,generate_msg(**msg_dic))
            if rc == False:
                return False
            rc=self.recv_msg(self.index_fd)
            if rc is None:
                return False
            rc_dic=resolve_msg(rc)
            if "super_node_id" in rc_dic:
                self.node_id=rc_dic["super_node_id"]
            #print self.node_id
            ddn_log.ddn_log("super node register\nnode_id:%s\n"%(self.node_id))
            return ("status code"  in rc_dic) and ((rc_dic["status code"] == 200) or (rc_dic["status code"] == 301) )
            pass
        elif self.role == ddn_role.role_gene:
            msg_dic["message_type"]="super_node_request"
            msg_dic["recommand_super_node"]=self.upper_ip
            msg_dic["ban_super_node_list"]=list()
            while True:
                rc=self.send_msg(self.index_fd,generate_msg(**msg_dic))
                #message sending failure
                if rc == False:
                    return False
                #message recving failure
                rc=self.recv_msg(self.index_fd)
                if rc is None:
                    return False
                rc_dic=resolve_msg(rc)
                #get uuid from index server
                #print rc_dic["status code"]
                if "status code" not in rc_dic or rc_dic["status code"] != 200:
                    return False
                if "target_node_ip"  not in rc_dic or "uuid" not in rc_dic or "allocated_super_node" not in rc_dic:
                    return False
                tmp_ip=rc_dic["target_node_ip"]
                tmp_uuid=rc_dic["uuid"]
                tmp_node_id=rc_dic["allocated_super_node"]
                #try to register node to the super node,if success, return
                self.upper_fd=self.get_tcp_socket_fd(tmp_ip,self.upper_port)
                if self.upper_fd is None:
                    msg_dic["ban_super_node_list"].append(tmp_ip)
                    continue
                local_msg=dict()
                local_msg["message_type"]="gene_node_register"
                local_msg["uuid"]=tmp_uuid
                rc=self.send_msg(self.upper_fd,generate_msg(**local_msg))
                if rc ==False:
                    self.upper_fd.close()
                    msg_dic["ban_super_node_list"].append(tmp_ip)
                    continue
                rc=self.recv_msg(self.upper_fd)
                rc_dic=resolve_msg(rc)
                if rc is None or "status code" not in rc_dic or "gene_node_id" not in rc_dic or "uuid" not in rc_dic \
                        or rc_dic["status code"] != 200:
                    self.upper_fd.close()
                    msg_dic["ban_super_node_list"].append(tmp_ip)
                    continue
                #print rc_dic
                #register succeed,return imediate
                self.upper_ip=tmp_ip
                self.uuid=tmp_uuid
                self.upper_node_id=tmp_node_id
                self.node_id=rc_dic["gene_node_id"]
                ddn_log.ddn_log("gene node register\nsuper_ip:%s\nsuper_node_id:%s\ngene_uuid:%s\ngene_id:%s\n"%
                                (self.upper_ip,self.upper_node_id,self.uuid,self.node_id))
                return True
            pass
        else:
            return False
        pass


if __name__=="__main__":
    #client_entity(ddn_role.role_super,index_ip="130.140.25.1").register_node()
    ce=client_entity(ddn_role.role_gene,index_ip="130.140.25.1").register_node()

