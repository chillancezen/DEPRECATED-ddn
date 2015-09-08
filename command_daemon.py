import socket
import threading
import json
import error_code
import node_mng

__author__ = 'jzheng'


def start_daemon(addr="0.0.0.0", port=507, node_role="node_index"):
    """
    generate and node management entity and start to resolve underlay user request
    :param addr:
    :param port:
    :return:None

    """
    nme = node_mng.node_mng_entity(node_role=node_role)  # generate node management entity

    fd = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fd.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    fd.bind((addr, port))
    fd.listen(5)

    while True:
        arg = fd.accept()
        arg += tuple([nme])
        threading.Thread(target=cmd_worker, kwargs={"arg": arg}).start()


def generate_return_msg(code,**attr):
    dic = dict()
    dic["status code"] = code
    if code in error_code.error_dict:
        dic["status msg"] = error_code.error_dict[code]
    else:
        dic["status msg"] = "not specified"
    for item in attr:
        if item not in dic:
            dic[item]=attr[item]
    msg = ""
    try:
        msg = json.dumps(dic, indent=2)
    except:
        msg = ""
    return msg


def super_node_register_cb(arg):
    """
    the message can be resolved by index_node exclusively
    :param arg:arg should be tuple like(parameter from cmd worker,,parameter from user request)
    :return: None
    """
    fd,addr,nme,para=arg
    if nme.role is not "node_index":
        fd.send(generate_return_msg(401))
        return
    node=nme.node_find_by_addr(addr[0])
    if  node:
        fd.send(generate_return_msg(301,**{"super_node_id":node.node_id,"request_ip":node.addr}))
    else:
        node=nme.node_alloc(node_mng.node_info,**{"id":nme.node_id_allocate(),"addr":addr[0]})
        nme.node_register(node.node_id,node)
        fd.send(generate_return_msg(200,**{"super_node_id":node.node_id,"request_ip":node.addr}))
    pass

def super_node_update_cb(arg):
    """
    called by index ndoe
    :param arg:should be tuple like(parameter from cmd worker,,parameter from user request)
    :return:None
    """
    fd,addr,nme,para=arg
    if nme.role is not "node_index":
        fd.send(generate_return_msg(401))
        return
    node=nme.node_find_by_addr(addr[0])
    if not node:
        fd.send(generate_return_msg(404))
    else:
        nme.node_update(node.node_id)
        fd.send(generate_return_msg(200))

def super_node_unregister_cb(arg):
    """
    called by index ndoe
    :param arg:
    :return:
    """
    fd,addr,nme,para=arg
    if nme.role is not "node_index":
        fd.send(generate_return_msg(401))
        return
    node=nme.node_find_by_addr(addr[0])
    if node:
        nme.node_delete(node.node_id)
        fd.send(generate_return_msg(200))
    else:
        fd.send(generate_return_msg(404))
    pass
def super_node_request_cb(arg):
    """
    called by index ndoe
    :param arg:
    :return:
    """
    fd,addr,nme,para=arg
    if nme.role is not "node_index":
        fd.send(generate_return_msg(401))
        return
    # validate parameter
    if "ban_super_node_list" in para and type(para["ban_super_node_list"]) is not  list:
        fd.send(generate_return_msg(400))
        return

    #decode these parameters
    recommand_node=None
    if "recommand_super_node" in para:
        recommand_node=para["recommand_super_node"]
    ban_list=list()
    if "ban_super_node_list" in para:
        for ele in para["ban_super_node_list"]:
            ban_list.append(ele)

    legal_node=None
    nme.nme_lock.acquire()
    for id in nme.node_tbl:
        node=nme.node_tbl[id]
        if node.addr in ban_list:
            continue
        #try to find least loaded node
        if node.addr ==  recommand_node:
            legal_node = node
            break
        if not legal_node:
            legal_node = node
        else:
            if node.client_cnt < legal_node.client_cnt:
                legal_node = node
    nme.nme_lock.release()

    if not legal_node:

        fd.send(generate_return_msg(404))
    else:
        legal_node.client_cnt += 1
        local_uuid=nme.node_uuid_allocate()
        #print type(local_uuid)
        fd.send(generate_return_msg(200,**{"uuid":str(local_uuid),"allocated_super_node":legal_node.node_id,"target_node_ip":legal_node.addr}))
    pass

def gene_node_register_cb(arg):
    """
    called by super node
    :param arg:
    :return:
    """
    fd,addr,nme,para=arg
    if nme.role is not "node_super":
        fd.send(generate_return_msg(401))
        return

    #validate uuid string
    if "uuid" not in para or len(para["uuid"]) is not 36:
        fd.send(generate_return_msg(400))
        return
    node=nme.node_find_by_uuid(para["uuid"])
    if node:
        #already registered
        fd.send(generate_return_msg(301,**{"uuid":node.uuid,"gene_node_id":node.node_id}))
    else:
        node=nme.node_alloc(node_mng.node_info,**{"id":nme.node_id_allocate(),"uuid":para["uuid"]})
        nme.node_register(node.node_id,node)
        fd.send(generate_return_msg(200,**{"uuid":node.uuid,"gene_node_id":node.node_id}))
        #help(node)
    pass
def gene_node_update_cb(arg):
    """
    called by super node
    :param arg:
    :return:
    """
    fd,addr,nme,para=arg
    if nme.role is not "node_super":
        fd.send(generate_return_msg(401))
        return
    if "uuid" not in para:
        fd.send(generate_return_msg(400))
        return
    node=nme.node_find_by_uuid(para["uuid"])
    if node :
        nme.node_update(node.node_id)
        fd.send(generate_return_msg(200))
    else:
        fd.send(generate_return_msg(404))
    pass
def gene_node_unregister_cb(arg):
    """
    called by super node
    :param arg:
    :return:
    """
    fd,addr,nme,para=arg
    if nme.role is not "node_super":
        fd.send(generate_return_msg(401))
        return
    if "uuid" not in para:
        fd.send(generate_return_msg(400))
        return
    node=nme.node_find_by_uuid(para["uuid"])
    if node:
        nme.node_delete(node.node_id)
        fd.send(generate_return_msg(200))
    else:
        fd.send(generate_return_msg(404))
    pass

action_dict = {
    "super_node_register": super_node_register_cb,
    "super_node_update":super_node_update_cb,
    "super_node_unregister":super_node_unregister_cb,
    "super_node_request":super_node_request_cb,
    "gene_node_register":gene_node_register_cb,
    "gene_node_update":gene_node_update_cb,
    "gene_node_unregister":gene_node_unregister_cb
}


def cmd_worker(arg):
    """
    :param arg: should be tupe like(socketfd,addr_tuple,nme)
    :return:
    """
    fd, addr, nme = arg

    while True:
        msg = fd.recv(1460)
        if not msg:  # link torn down
            break
        jmsg = None
        try:
            jmsg = json.loads(msg)
        except:
            jmsg = None
            fd.send(generate_return_msg(400))
        if jmsg is None:
            continue
        if "message_type" not in jmsg:
            fd.send(generate_return_msg(400))
            continue
        msg_cmd=jmsg["message_type"]
        if msg_cmd not in action_dict:#message entry point not found
            fd.send(generate_return_msg(401))
            continue
        #message entry found
        args=arg
        args+=tuple([jmsg])
        action_dict[msg_cmd](args)

    fd.close()


if __name__ == "__main__":
    # help(tuple)
    start_daemon(node_role="node_super")
