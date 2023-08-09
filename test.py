from tests import pytest_hosts
from ez_zk_client import ZkClient, KazooState
import time

client = None
listen_node_data = "for pytest"
listen_node_new_data = 'change data'
listen_node_path = "/testing/test_listener"
listen_node_children_path = '/testing/listen_children'
listen_children_data=[]
listen_after_change_children_data=['child2', 'child1']
client = ZkClient(hosts=pytest_hosts)
client.connect(True)
node_data = "for pytest"
client.ensure_path(listen_node_children_path)

def children_change(node_path,data):
    global listen_children_data
    print('change',data)
    listen_children_data = data

client.add_children_listener(listen_node_children_path,children_change)
client.alive(f'{listen_node_children_path}/child1')
client.alive(f'{listen_node_children_path}/child2')
time.sleep(3)
print(listen_children_data , listen_after_change_children_data)
client.disconnect()
client = None