'''
   Basic implementation a distributed lock mechanism using zookeeper
   It allows to :
            * Acquire the lock using an ephemeral sequential node
            * Realease the lock by deleting the ephemeral node
            * Optionnaly check if the lock is hold by another host
'''

import time
from kazoo.client import KazooClient , ChildrenWatch , DataWatch
from kazoo.exceptions import KazooException

class Locks :
    def __init__(self,host_id):
        # Set the host_id
        self.host_id = host_id
        self.root_node = "/locks"
        try :
            self.zk = KazooClient(hosts = "172.28.112.1:2181")
            self.zk.start()

            # If the root node not exists create it
            if self.zk.exists(self.root_node) is None : # zookeeper exception
                self.zk.create (self.root_node ,
                                b"root node" ,
                                ephemeral = False) # Perhaps create it with acls

            # Create a sequential ephemere child node holding host_id a s data
            self.zk.create(self.root_node + "/lock-" ,
                           self.host_id.encode() ,
                           ephemeral = True ,
                           sequence = True)
            self.set_children_watcher()

        except KazooException as ke :
            print(f"There was an error : {ke}")

    # Watcher on the ephemere child node
    def eph_child_watcher (self , data , stat , event):
        if event is not None :
            print(f"{event.path}  was  {event.type}")
        else :
            print("Initial Call")

    # Set a watcher on the ephemere child node
    def set_eph_node_watcher (self , first_child):
        DataWatch(self.zk  , first_child , self.eph_child_watcher , send_event = True)


    # Call back function when there is update on childs of root node
    def node_watcher(self, children , event):
        '''
        Function to call when there is an update on the childrens
        '''
        print(f"Child was created or deleted : list of actual children : {children}" )

    # Set a watcher on the root node
    def set_children_watcher(self ):
        '''
        Set a watcher
        '''
        ChildrenWatch(self.zk, self.root_node, self.node_watcher , send_event = True)

    # Define a function to acquire the lock
    def acquire_or_wait(self , _host_id , _root_node):
        '''
        Method to acquire or wait to acquire the lock
        '''
        # get the list of children and the first children of the list
        children = self.zk.get_children (_root_node)
        sorted_children  = sorted(children)
        if len (sorted_children) != 0 :
            first_children = _root_node + "/" + sorted_children[0]
            self.set_eph_node_watcher(first_children)
            data_first_children = self.zk.get(first_children)[0].decode("utf-8")
            if data_first_children == self.host_id :
                print("I acquire the lock ")
                for i in range (10):
                    t = 10 - i
                    print (f"i will leave the lock in {t} seconds")
                    time.sleep(1)
                self.zk.delete(first_children)
            else :
                print("I could not have a lock so i will wait")
                time.sleep(2)
                self.acquire_or_wait(_host_id , _root_node)
        else :
            print("No one is waiting for lock")
