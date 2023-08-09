from ez_zk_client import ZkClient, KazooState
from tests import pytest_hosts
import time

client = None
listen_node_data = "for pytest"
listen_node_new_data = 'change data'
listen_node_path = "/testing/test_listener"
listen_node_children_path = '/testing/listen_children'
listen_children_data=[]
listen_after_change_children_data=['child2', 'child1']
def setup_function():
    print("run setup_function")
    global client, listen_node_path, listen_node_data,listen_node_children_path
    client = ZkClient(hosts=pytest_hosts)
    client.connect(True)
    node_data = "for pytest"
    client.create_node(listen_node_path, listen_node_data)    
    client.add_node_listener(listen_node_path, node_data_change)
    client.ensure_path(listen_node_children_path)
    client.add_children_listener(listen_node_children_path,children_change)

def teardown_function():
    global client
    client.disconnect()
    client = None

def children_change(node_path,data):
    global listen_children_data
    print('change',data)
    listen_children_data = data

def node_data_change(node_path, data):
    global listen_node_data
    listen_node_data = data

def test_listen():
    global client, listen_node_path, listen_node_data,listen_node_new_data
    client.set_data(listen_node_path, listen_node_new_data)
    time.sleep(1)  # after event fire
    assert listen_node_data == listen_node_new_data

def test_children_change():
    global client,listen_node_children_path,listen_children_data,listen_after_change_children_data
    client.alive(f'{listen_node_children_path}/child1')
    client.alive(f'{listen_node_children_path}/child2')
    time.sleep(3)
    assert ','.join(listen_children_data) == ','.join(listen_after_change_children_data)
if __name__ == "__main__":
    # test_listen()
    setup_function()
