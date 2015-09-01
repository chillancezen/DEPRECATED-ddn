#! /usr/bin/python
import random
import uuid
import threading

class node_info:

	"""
	node information which identifies the unique underlayer node
	"""
	def __init__(self,id,uuid=None,addr=None):
		self.node_id=id
		self.uuid=uuid #identifier used by super node
		self.addr=addr# identifier used by index node
		self.timer_cnt=0
		
	def node_init_callback(self):
		
		pass
	def node_delete_callback(self):

		pass
	def node_expiry_callback(self):
		print "expiry"
		pass
class node_mng_entity:
	"""
	node management entity,there should be only one 
	instance on index node or super node
	"""
	age_exit_flag=False
	nme_lock=threading.Lock()
	node_tbl=dict()
	node_expiry_time=20 #seconds age timers
	node_id_pool=set()
	node_uuid_pool=set()
	def __init__(self):

		#self.lock=threading.Lock()
		#spawn the age timer threading
		self.age_pid=threading.Thread(target=age_timer,kwargs={"nme":self})
		self.age_pid.setDaemon(True)
		self.age_pid.start()


	def random_string(self,len):#generate random string
		str=""
		for i in range(len):
			str+=random.choice("0123456789abcdef")
		return str
	def node_id_allocate(self):
		str=""
		while True:
			str=self.random_string(8)
			if str not in self.node_id_pool:
				break
		self.node_id_pool.add(str)
		return str
	def node_uuid_allocate(self):
		uid=""
		while True:
			uid=uuid.uuid4()
			if uid  not in self.node_uuid_pool:
				break
		self.node_uuid_pool.add(uid)
		return uid

	def node_alloc(self,node_type,**arg):
		return node_type(**arg)	
		pass

	def node_find(self,key_id):
		if key_id not in self.node_tbl:
			return None
		else :
			return self.node_tbl[key_id]

	
	def node_register(self,key_id,node):
		if key_id  in self.node_tbl:
			return False
		self.node_tbl[key_id]=node
		return True

	def node_delete(self,key_id):
		if self.node_find(key_id) is not None:
			self.node_tbl[key_id].node_delete_callback()#call delete routine
			del self.node_tbl[key_id]
	def node_update(self,key_id):
		self.nme_lock.acquire()
		if key_id in self.node_tbl:
			self.node_tbl[key_id].timer_cnt=0
		self.nme_lock.release()
		pass

	def stop_age_timer(self):
		self.age_exit_flag=True
		self.age_pid.join()#wait for thread finishing

def age_timer(nme):
	"""
		age timer thread
	"""
	interval_time=2

	while nme.age_exit_flag is False:#thread not safe,but it does no matter
		threading._sleep(interval_time)
		nme.nme_lock.acquire()
		del_lst=set()
		#traverse the node table,fidn these expired node,and recollect them
		for nod in nme.node_tbl:
			if nme.node_tbl[nod].timer_cnt > nme.node_expiry_time:
				nme.node_tbl[nod].node_expiry_callback()
				del_lst.add(nod)
			else :
				nme.node_tbl[nod].timer_cnt+=interval_time
		for ele in del_lst:
			if ele in nme.node_tbl:
				del nme.node_tbl[ele]
		nme.nme_lock.release()

if __name__ == "__main__":
	nme=node_mng_entity()
	node=nme.node_alloc(node_info,**{"id":nme.node_id_allocate(),"addr":"192.168.6.1"})
	nme.node_register(node.node_id,node)
	#nme.node_delete(node.node_id)
	threading._sleep(500)
	nme.stop_age_timer()
	node_bak=nme.node_find(node.node_id)
	print node_bak.node_id
	print nme.node_uuid_allocate()
	print nme.node_uuid_allocate()
	print nme.node_uuid_allocate()
