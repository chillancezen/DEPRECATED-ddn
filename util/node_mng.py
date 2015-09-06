#! /usr/bin/python
import random
import uuid
import threading
node_role=[
	"node_index",
	"node_super",
	"node_gene"
]
class node_info:

	"""
	node information which identifies the unique underlayer node
	"""
	def __init__(self,id,uuid=None,addr=None):
		self.node_id=id
		self.uuid=uuid #identifier used by super node
		self.addr=addr# identifier used by index node
		self.timer_cnt=0
		self.client_cnt=0

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
	nme_lock=threading.RLock()
	node_tbl=dict()
	node_expiry_time=300 #seconds age timers
	node_id_pool=set()
	node_uuid_pool=set()
	role=None
	def __init__(self,node_role="node_index"):

		#self.lock=threading.Lock()
		#spawn the age timer threading
		self.age_pid=threading.Thread(target=age_timer,kwargs={"nme":self})
		self.age_pid.setDaemon(True)
		self.age_pid.start()
		self.role=node_role

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

	def node_find(self,key_id):#lock required
		self.nme_lock.acquire()
		if key_id not in self.node_tbl:
			self.nme_lock.release()
			return None
		else :
			self.nme_lock.release()
			return self.node_tbl[key_id]
	def node_find_by_addr(self,addr):#lock required
		# find ndoe according to  addr, especially called by index ndoe
		self.nme_lock.acquire()
		for item in self.node_tbl:
			if self.node_tbl[item].addr == addr:
				self.nme_lock.release()
				return self.node_tbl[item]
		self.nme_lock.release()
		return None
	def node_find_by_uuid(self,uuid):#lock required
		# find ndoe according to  addr, especially called by super ndoe
		self.nme_lock.acquire()
		for item in self.node_tbl:
			if self.node_tbl[item].uuid == uuid:
				self.nme_lock.release()
				return self.node_tbl[item]
		self.nme_lock.release()
		return None

	def node_register(self,key_id,node):#lock required
		self.nme_lock.acquire()
		if key_id  in self.node_tbl:
			self.nme_lock.release()
			return False
		self.node_tbl[key_id]=node
		self.nme_lock.release()
		return True

	def node_delete(self,key_id):#lock required
		self.nme_lock.acquire()
		if self.node_find(key_id) is not None:
			self.node_tbl[key_id].node_delete_callback()#call delete routine
			#delete uuid and node_id in id-pool and uuid in uuid-pool
			self.node_id_pool.discard(key_id)
			self.node_uuid_pool.discard(self.node_tbl[key_id].uuid)
			del self.node_tbl[key_id]
		self.nme_lock.release()

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
		#print "meeeow"
		#traverse the node table,fidn these expired node,and recollect them
		for nod in nme.node_tbl:
			if nme.node_tbl[nod].timer_cnt > nme.node_expiry_time:
				nme.node_tbl[nod].node_expiry_callback()
				del_lst.add(nod)
			else :
				nme.node_tbl[nod].timer_cnt+=interval_time
		for ele in del_lst:
			if ele in nme.node_tbl:
				#nme.node_id_pool.discard(ele)
				#nme.node_uuid_pool.discard(nme.node_tbl[ele].uuid)
				nme.node_delete(ele)
				#del nme.node_tbl[ele]
				#nme.node_delete(ele)
		#help(nme)
		nme.nme_lock.release()

if __name__ == "__main__":
	"""
	nme=node_mng_entity()
	node=nme.node_alloc(node_info,**{"id":nme.node_id_allocate(),"addr":"192.168.6.1","uuid":nme.node_uuid_allocate()})
	nme.node_register(node.node_id,node)
	print "meee"
	#nme.node_delete(node.node_id)
	print "meeeow"
	#threading._sleep(500)
	help(nme)
	print nme.node_find_by_addr("192.168.6.1").uuid
	nme.stop_age_timer()
	node_bak=nme.node_find(node.node_id)
	print node_bak.node_id
	print nme.node_uuid_allocate()
	print nme.node_uuid_allocate()
	print nme.node_uuid_allocate()
	"""
	pass
