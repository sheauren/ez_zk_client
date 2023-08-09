# ez_zk_client install
```python
pip install ez_zk_client
```
# example connect/disconnect
```python
from ez_zk_client import ZkClient
client = ZkClient(hosts='localhost:2181')
#client.connect(wait_connected=True)
client.connect()
client.disconnect()
```
# example create_path
```python
client.create_node('/pytest','for pytest')
```

# example create_ephemeral & get value/status
```python
client.alive('/pytest/online','create oneline path, offlne auto disapear')
val,stat =  client.get_data('/pytest/online')
```
# example listen node
```python
def node_data_change(node_path, data):
    print('data change:',data) # new data

client.create_node("/testing/test_listener", 'old data')    
client.add_node_listener("/testing/test_listener", node_data_change)
client.set_data("/testing/test_listener", "new data")
```
# example listen children
```python
def children_change(node_path,data):
    # fire twice time
    print(data) # children list: ['child1'] in first time
    print(data) # children list: ['child1','child2'] in second time

client.ensure_path('/testing/listen_children')
client.add_children_listener('/testing/listen_children',children_change)
client.alive(f'/testing/listen_children/child1')
client.alive(f'/testing/listen_children/child2')
    
```

# example lock single resource
```python
client.alive("/testing/single_resource/ALIVE", 'http://localhost:3388')
locker = client.lock_single_resource("/testing/single_resource/ALIVE")
# locker.path=='/testing/single_resource'
with locker: # auto release
    api,stat = client.get_data(locker.path)
    # access api

```
# example lock pool resource
```python
inserted_path = client.insert_pool_resource('/testing/resource_pool', 'http://localhost:10001')
inserted_path = client.insert_pool_resource('/testing/resource_pool', 'http://localhost:10002')
inserted_path = client.insert_pool_resource('/testing/resource_pool', 'http://localhost:10003')
locker1 = client.use_pool_resource('/testing/resource_pool')
api1,stat = client.get_data(locker.path) # http://localhost:10001
locker2 = client.use_pool_resource('/testing/resource_pool')
api2,stat = client.get_data(locker.path) # http://localhost:10002
locker3 = client.use_pool_resource('/testing/resource_pool')
api3,stat = client.get_data(locker.path) # http://localhost:10003
```