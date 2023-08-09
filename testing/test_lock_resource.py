from ez_zk_client import ZkClient, KazooState
from tests import pytest_hosts
import time

client = None
listen_node_data = "for pytest"
listen_resource_path = "/testing/single_resource"
listen_resource_pool_path= '/testing/resource_pool'
listen_resource_alive_data = 'http://localhost:3388'
listen_resource_pool1_data = 'http://localhost:10001'
listen_resource_pool2_data = 'http://localhost:10002'
listen_resource_pool3_data = 'http://localhost:10003'

def setup_function():
    print("run setup_function")
    global client, listen_resource_path, listen_resource_pool_path,listen_resource_alive_data,listen_resource_pool1_data,listen_resource_pool2_data,listen_resource_pool3_data
    client = ZkClient(hosts=pytest_hosts)
    client.connect(True)
    client.alive(f"{listen_resource_path}/ALIVE", listen_resource_alive_data)
    client.insert_pool_resource(listen_resource_pool_path, listen_resource_pool1_data)
    client.insert_pool_resource(listen_resource_pool_path, listen_resource_pool2_data)
    client.insert_pool_resource(listen_resource_pool_path, listen_resource_pool3_data)    

def teardown_function():
    global client
    client.disconnect()
    client = None

def lock_single_resource():
    global client,listen_resource_path
    locker = client.lock_single_resource(listen_resource_path)
    assert locker.path=='/testing/single_resource'
    with locker: # auto release
        time.sleep(1)


def get_pool_resource():
    global client,listen_resource_pool_path
    locker1 = client.use_pool_resource(listen_resource_pool_path)
    locker2 = client.use_pool_resource(listen_resource_pool_path)
    locker3 = client.use_pool_resource(listen_resource_pool_path)
    time.sleep(1)
    assert locker1.path=='/testing/resource_pool/POOL/1'
    assert locker2.path=='/testing/resource_pool/POOL/2'
    assert locker3.path=='/testing/resource_pool/POOL/3'
    locker1.release()
    locker2.release()
    locker3.release()



if __name__ == "__main__":
    # test_listen()
    setup_function()
