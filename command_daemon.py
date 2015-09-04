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
    pass
action_dict = {
    "super_node_register": super_node_register_cb,
    "super_node_update":super_node_update_cb
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
    start_daemon()
