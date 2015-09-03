import  socket
import threading
import json
import error_code

__author__ = 'jzheng'

def start_daemon(addr="0.0.0.0",port=507):
    """
    generate and node management entity and start to resolve underlay user request
    :param addr:
    :param port:
    :return:None

    """
    fd=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    fd.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    fd.bind((addr,port))
    fd.listen(5)

    while True:
        arg=fd.accept()
        arg += tuple([123])
        threading.Thread(target=cmd_worker,kwargs={"arg":arg}).start()


def generate_return_msg(code):
    dic=dict()
    dic["status code"]=code
    if code in error_code.error_dict:
        dic["status msg"]=error_code.error_dict[code]
    else:
        dic["status msg"]="not specified"
    msg=""
    try:
        msg=json.dumps(dic,indent=2)
    except:
        msg = ""
    return msg

def cmd_worker(arg):
    """
    :param arg: should be tupe like(socketfd,addr_tuple,nme)
    :return:
    """
    fd,addr,nme=arg
    while True:
        msg=fd.recv(1460)
        if not msg:#link torn down
            break
        jmsg=None
        try:
            jmsg=json.loads(msg)
        except:
            jmsg=None
            fd.send(generate_return_msg(400))
        print jmsg

    fd.close()
    pass
if __name__ == "__main__":
   # help(tuple)
    start_daemon()