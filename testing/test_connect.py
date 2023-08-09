import os
import sys
sys.path.append('../')
from ez_zk_client import ZkClient,KazooState
from tests import pytest_hosts
#pytest_hosts='172.100.100.200:2181'

def test_connect():
    client = ZkClient(hosts=pytest_hosts)
    assert client.connect(wait_connected=True)==KazooState.CONNECTED
    client.disconnect()

def test_create_path():
    client = ZkClient(hosts=pytest_hosts)
    client.connect(wait_connected=True)==KazooState.CONNECTED
    client.create_node('/pytest','for pytest')
    client.disconnect()    

def test_create_ephemeral():
    client = ZkClient(hosts=pytest_hosts)
    client.connect(wait_connected=True)==KazooState.CONNECTED
    client.alive('/pytest/online','create oneline path, offlne auto disapear')
    val,stat =  client.get_data('/pytest/online')
    assert val=='create oneline path, offlne auto disapear'
    client.disconnect()    

def test_get_path_data():
    client = ZkClient(hosts=pytest_hosts)
    client.connect(wait_connected=True)==KazooState.CONNECTED
    val,stat = client.get_data('/pytest')
    assert val=='for pytest'
    client.disconnect()


if __name__=='__main__':
    test_connect()