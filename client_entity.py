import threading
import json

__author__ = 'jzheng'
class client_entity:
    """
    client entity which represents gene or super node in the delivery network
    the should be instantiated by gene node or super node

    """
    role=None
    def __init__(self,role):
        self.role=role
        pass

if __name__ == "__main__":
    client_entity(role="node_gene")
    