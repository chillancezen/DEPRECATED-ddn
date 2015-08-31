#! /usr/bin/python
import random
class node_info:
	"""
	node information which identifies the unique underlayer node
	"""
	def __init__(self,id,uuid=None,addr=None):
		self.node_id=id
		self.uuid=uuid
		self.addr=addr
		self.timer_cnt=0
		
	def node_init_callback(self):
		
		pass
	def node_delete_callback(self):
	
		pass
	def node_expiry_callback(self):

		pass
class node_mng_entity:
	"""
	node management entity,there should be only one 
	instance on index node or super node
	"""
	def __init__(self):
		self.node_tbl=dict()
		self.node_expiry_time=300 #seconds age timers
		self.node_id_pool=set()
		self.node_uuid_pool=set()

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
			del self.node_tbl[key_id]
	def node_update(self):

		pass
	def age_timer(self):
		"""
		age timer thread 
		"""
		pass

if __name__ == "__main__":
	nme=node_mng_entity()
	node=nme.node_alloc(node_info,**{"id":nme.node_id_allocate(),"addr":"192.168.6.1"})
	nme.node_register(node.node_id,node)
	nme.node_delete(node.node_id)

	node_bak=nme.node_find(node.node_id)
	print node_bak.node_id
