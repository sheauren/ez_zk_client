from kazoo.client import KazooClient, KazooState, WatchedEvent, ACL
from kazoo import security
from collections import defaultdict
from functools import partial
from typing import Callable
from random import choice
import time
import sys
import threading


def create_acl(username, password):
    return [security.make_digest_acl(username, password, all=True)]


class ZkClient(object):
    zk_client_dict: dict = dict()
    state: KazooState = KazooState.LOST
    skip_reconnect_one_time = False

    def __init__(
        self,
        name="default",
        hosts="localhost:2181",
        security: bool = False,
        keyfile: str = None,
        keyfile_password: str = None,
        certfile: str = None,
        default_acl=None,  # create_acl(),
        randomize_hosts: str = True,
        auth_data: list = None,  # = [("digest", "username:pwd")],
        auto_reconnect: bool = True,
    ):
        self.name = name
        self.hosts = hosts
        self.security = security
        self.keyfile = keyfile
        self.keyfile_password = keyfile_password
        self.certfile = certfile
        self.default_acl = default_acl
        self.randomize_hosts = randomize_hosts
        self.auth_data = auth_data
        self.kazoo_client = None
        self.auto_reconnect = auto_reconnect
        self.state = KazooState.LOST
        self.skip_reconnect_one_time = False

    def connect(self, wait_connected=False):
        self.kazoo_client = connect(
            self.name,
            self.hosts,
            self.security,
            self.keyfile,
            self.keyfile_password,
            self.certfile,
            self.default_acl,
            self.randomize_hosts,
            self.auth_data,
            self.state_change,
        )
        if wait_connected:
            for _ in range(50):
                time.sleep(0.2)
                if self.state == KazooState.CONNECTED:
                    break
        return self.state

    def state_change(self, state):
        print(f"zk client {self.name} state change: {state}")
        self.state = state
        if self.state == KazooState.LOST and self.auto_reconnect:  # retry to reconnect
            if self.skip_reconnect_one_time:
                self.skip_reconnect_one_time = False
            else:
                print("zk client auto reconnecting...")
                threading.Timer(5.0, self.__reconnect).start()

    def __reconnect(self):
        if self.state == KazooState.LOST and self.auto_reconnect:
            self.connect()
            threading.Timer(10.0, self.__reconnect).start()

    def get_client(self):
        return get_client(self.name)

    def disconnect(self):
        if self.auto_reconnect:
            self.skip_reconnect_one_time = True
        if self.state == KazooState.CONNECTED:
            self.kazoo_client.stop()

    def __del__(self):
        disconnect(self.name)
        self.kazoo_client = None

    def create_acl(self, username, password):
        return create_acl(username, password)

    def lock_single_resource(
        self, resource_path: str, data: str = None, timeout: int = 10
    ):
        return lock_single_resource(resource_path, data, timeout, self.name)

    def use_pool_resource(self, resource_path, data=None, timeout:int=10, nolock=False):
        return use_pool_resource(resource_path, data, timeout, nolock, self.name)

    def insert_pool_resource(self, resource_path: str, data: str = None):
        return insert_pool_resource(resource_path, data, self.name)

    def lock(self, resource_path: str, data: str = None, timeout: int = 10):
        return lock(resource_path, data, timeout, self.name)

    def ensure_path(self, node_path: str):
        return ensure_path(node_path, self.name)

    def create_node(self, node_path: str, data: str = None, watch: Callable = None):
        return create_node(node_path, data, watch, self.name)

    def set_data(self, node_path: str, data: str, version: int = -1):
        return set_data(node_path, data, version, self.name)

    def get_data(self, node_path: str, watch: Callable = None):
        return get_data(node_path, watch, self.name)

    def alive(self, node_path: str, data: str = None):
        return alive(node_path, data, self.name)

    def set_lock_data(self, node_path: str, data: str, version=-1):
        return set_lock_data(node_path, data, version, self.name)

    def get_lock_data(self, node_path: str):
        return get_lock_data(node_path, self.name)

    def add_lock_listener(self, node_path: str, cb: Callable):
        return add_lock_listener(node_path, cb, self.name)

    def remove_lock_listener(self, node_path: str, cb: Callable):
        return remove_lock_listener(node_path, cb, self.name)

    def add_node_listener(self, node_path: str, cb: Callable):
        return add_node_listener(node_path, cb, self.name)

    def remove_node_listener(self, node_path: str, cb: Callable = None):
        return remove_node_listener(node_path, cb, self.name)

    def add_children_listener(self, node_path: str, cb: Callable = None):
        return add_children_listener(node_path,  cb,self.name)

    def remove_children_listener(self, node_path: str, cb: Callable = None):
        return remove_children_listener(node_path, cb, self.name)


def get_client(name="default"):
    if name in ZkClient.zk_client_dict:
        return ZkClient.zk_client_dict[name]
    else:
        raise Exception(f"doesn't has {name} ZkClient")


def connect(
    name: str = "default",
    hosts="localhost:2181",
    security: bool = False,
    keyfile: str = None,
    keyfile_password: str = None,
    certfile: str = None,
    default_acl=None,
    randomize_hosts: str = True,
    auth_data: list = None,  # list = [("digest", "username:pwd")]
    state_change: Callable = None,
):
    if name not in ZkClient.zk_client_dict:
        if security:
            ZkClient.zk_client_dict[name] = KazooClient(
                hosts=hosts,
                use_ssl=True,
                verify_certs=False,
                keyfile=keyfile,
                certfile=certfile,
                keyfile_password=keyfile_password,
                randomize_hosts=randomize_hosts,
                auth_data=auth_data,
                default_acl=default_acl,
            )
        else:
            ZkClient.zk_client_dict[name] = KazooClient(
                hosts=hosts,
                randomize_hosts=randomize_hosts,
                auth_data=auth_data,
                default_acl=default_acl,
            )
        ZkClient.zk_client_dict[name].add_listener(state_change)
    try:
        ZkClient.zk_client_dict[name].start()
    except Exception as ex:
        print(sys.exc_info()[0])
        del ZkClient.zk_client_dict[name]
        raise ex
    return ZkClient.zk_client_dict[name]


def disconnect(name="default", delete=False):
    if name in ZkClient.zk_client_dict:
        if ZkClient.zk_client_dict[name].state == KazooState.CONNECTED:
            ZkClient.zk_client_dict[name].stop()
    if delete:
        del ZkClient.zk_client_dict[name]


def lock_single_resource(
    resource_path: str, data: str = None, timeout: int = 10, name="default"
):
    return lock(resource_path, data, timeout, name)


def use_pool_resource(
    resource_path: str,
    data: str = None,
    timeout: int = 10,
    nolock=False,
    name="default",
):
    if name not in ZkClient.zk_client_dict:
        raise Exception(
            f"{name} zk client not exist, can't use pool resource:{resource_path}"
        )
    pool_path = f"{resource_path}/POOL"
    lock_path = f"{resource_path}/LOCK"
    if not ZkClient.zk_client_dict[name].ensure_path(pool_path):
        raise Exception(f"{pool_path} not found")
    if not ZkClient.zk_client_dict[name].ensure_path(lock_path):
        raise Exception(f"{lock_path} not found")
    pool_list = ZkClient.zk_client_dict[name].get_children(pool_path)
    pool_size = len(pool_list)
    if pool_size == 0:
        raise Exception(f"{pool_path} don't have any pool resources in {pool_path}")
    if nolock:
        symbol = choice(pool_list)  # random pick one
        return f"{resource_path}/POOL/{symbol}"

    locker = ZkClient.zk_client_dict[name].Lock(resource_path)
    locker.acquire(timeout=timeout)    
    (resource_val, resource_stat) = ZkClient.zk_client_dict[name].get(resource_path)
    
    lock_list = ZkClient.zk_client_dict[name].get_children(lock_path)
    lock_node_list = []
    lock_queue=''
    if resource_val:
        lock_queue=resource_val.decode('utf-8')
    for node_name in lock_list:
        if (
            len(ZkClient.zk_client_dict[name].get_children(f"{lock_path}/{node_name}"))
            == 0
        ):
            # remove path
            ZkClient.zk_client_dict[name].delete(f"{lock_path}/{node_name}")
            lock_queue = lock_queue.replace(node_name, "")
        else:
            lock_node_list.append(node_name)
    lock_num = len(lock_queue)
    if lock_num < pool_size:
        node_name = __get_unlock_pool_symbol(lock_queue, pool_list)
        lock_queue += node_name
    else:
        # pick first node & move to last
        node_name = lock_queue[0]
        lock_queue = lock_queue[1:] + node_name

    ZkClient.zk_client_dict[name].set(
        resource_path, lock_queue.encode("utf-8"), resource_stat.version
    )
    locker.release()
    locker = None
    if data:
        data.encode("utf-8")
    print('lock resource',f"{lock_path}/{node_name}")
    resource_locker = ZkClient.zk_client_dict[name].Lock(
        f"{lock_path}/{node_name}", data
    )
    resource_locker.acquire(timeout=timeout)
    return resource_locker


def insert_pool_resource(resource_path: str, data: str = None, name="default"):
    if name not in ZkClient.zk_client_dict:
        raise Exception(
            f"{name} zk client not exist, can't insert pool resource:{resource_path}"
        )

    pool_path = f"{resource_path}/POOL"
    if not ZkClient.zk_client_dict[name].ensure_path(pool_path):
        raise Exception(f"{pool_path} not found")

    pool_list = ZkClient.zk_client_dict[name].get_children(pool_path)
    pool_resource_name = __get_unused_pool_symbol(pool_list, len(pool_list) + 1)
    node_path = f"{pool_path}/{pool_resource_name}"
    if ZkClient.zk_client_dict[name].exists(node_path):
        ZkClient.zk_client_dict[name].delete(node_path)
    ZkClient.zk_client_dict[name].create(
        node_path, data.encode("utf-8"), ephemeral=True
    )
    return node_path


def lock(resource_path: str, data: str = None, timeout: int = 10, name="default"):
    if name in ZkClient.zk_client_dict:
        if data:
            data=data.encode('utf-8')
        locker = ZkClient.zk_client_dict[name].Lock(resource_path, data)
        return locker
    raise Exception(f"{name} zk client not exist, can't lock resource:{resource_path}")


def ensure_path(node_path: str, name="default"):
    if name in ZkClient.zk_client_dict:
        return ZkClient.zk_client_dict[name].ensure_path(node_path)
    raise Exception(f"{name} zk client not exist, can't ensure path:{node_path}")


def create_node(
    node_path: str, data: str = None, watch: Callable = None, name="default"
):
    if name not in ZkClient.zk_client_dict:
        raise Exception(f"{name} zk client not exist, can't create node:{node_path}")

    if not ZkClient.zk_client_dict[name].exists(node_path):
        parent_path = "/".join(node_path.split("/")[:-1])
        ensure_path(parent_path,name)
        ZkClient.zk_client_dict[name].create(node_path, data.encode("utf-8"))
    else:
        ZkClient.zk_client_dict[name].set(node_path, data.encode("utf-8"))
    if watch:
        add_node_listener(node_path, watch, name)


def set_data(node_path: str, data: str, version: int = -1, name="default"):
    if name not in ZkClient.zk_client_dict:
        raise Exception(f"{name} zk client not exist, can't set data:{node_path}")
    ZkClient.zk_client_dict[name].set(node_path, data.encode("utf-8"), version=version)


def get_data(node_path: str, watch: Callable = None, name="default"):
    if name not in ZkClient.zk_client_dict:
        raise Exception(f"{name} zk client not exist, can't get data:{node_path}")
    val, stat = ZkClient.zk_client_dict[name].get(node_path, watch)
    return val.decode("utf-8"), stat


def alive(node_path: str, data: str = None, name="default"):
    if name not in ZkClient.zk_client_dict:
        raise Exception(f"{name} zk client not exist, can't alive path:{node_path}")

    if ZkClient.zk_client_dict[name].exists(node_path):
        ZkClient.zk_client_dict[name].delete(node_path)
    else:
        # get parent node
        parent_path = "/".join(node_path.split("/")[:-1])
        if len(parent_path) != 0:
            if not ZkClient.zk_client_dict[name].exists(parent_path):
                ZkClient.zk_client_dict[name].ensure_path(parent_path)
    if data:
        data = data.encode("utf-8")
    ZkClient.zk_client_dict[name].create(
        node_path, data, ephemeral=True
    )


def set_lock_data(node_path: str, data: str, version=-1, name="default"):
    if name not in ZkClient.zk_client_dict:
        raise Exception(f"{name} zk client not exist, can't set lock data:{node_path}")

    children_list = ZkClient.zk_client_dict[name].get_children(node_path)
    if len(children_list) == 1:  # find it
        set_data(f"{node_path}/{children_list[0]}", data, version, name)
    else:
        raise Exception(f"{node_path}'s lock node not found")


def get_lock_data(node_path: str, name="default"):
    if name not in ZkClient.zk_client_dict:
        raise Exception(f"{name} zk client not exist, can't get lock data:{node_path}")

    children_list = ZkClient.zk_client_dict[name].get_children(node_path)
    if len(children_list) == 1:  # find it
        return get_data(f"{node_path}/{children_list[0]}")
    else:
        raise Exception(f"{node_path}'s lock node not found")


__node_listener = defaultdict(lambda: defaultdict(list))
__children_listener = defaultdict(lambda: defaultdict(list))
__watched_node_list = defaultdict(list)
__watched_children_list = defaultdict(list)


def __watch_node_event(node_path, name="default", data=None):
    if name not in ZkClient.zk_client_dict:
        raise Exception(f"{name} zk client not exist, can't watch node:{node_path}")

    if len(__node_listener[name][node_path]) == 0:
        # don't need watch again
        return

    if isinstance(data, WatchedEvent):
        if data.type == "DELETED":
            __watched_node_list[name].remove(node_path)
            data = None
        else:
            try:
                data,status = ZkClient.zk_client_dict[name].get(
                    node_path, partial(__watch_node_event, node_path,name)
                )
                if data:
                    data = data.decode('utf-8')
                else:
                    data=''
            except:
                pass
    for cb in __node_listener[name][node_path]:
        try:
            cb(node_path, data)
        except:
            pass


def __watch_children_event(node_path: str, name="default", data: str = None):
    if name not in ZkClient.zk_client_dict:
        raise Exception(
            f"{name} zk client not exist, can't watch children node:{node_path}"
        )

    if len(__children_listener[name][node_path]) == 0:
        return    
    data = ZkClient.zk_client_dict[name].get_children(
        node_path, partial(__watch_children_event, node_path, name)
    )

    for cb in __children_listener[name][node_path]:
        try:
            cb(node_path, data)
        except Exception as ex:
            print(f"debug:{ex}")
            pass


def add_lock_listener(node_path: str, cb: Callable, name="default"):
    if name not in ZkClient.zk_client_dict:
        raise Exception(
            f"{name} zk client not exist, can't add lock listener:{node_path}"
        )

    children_list = ZkClient.zk_client_dict[name].get_children(node_path)
    if len(children_list) == 1:
        lock_path = f"{node_path}/{children_list[0]}"
        print(f"listener lock path: {lock_path}")
        add_node_listener(node_path, cb, name)
    else:
        print(f"lock path not found in {node_path}")


def remove_lock_listener(node_path: str, cb: Callable, name="default"):
    if name not in ZkClient.zk_client_dict:
        raise Exception(
            f"{name} zk client not exist, can't remove lock listener:{node_path}"
        )
    children_list = ZkClient.zk_client_dict[name].get_children(node_path)
    if len(children_list) == 1:
        lock_path = f"{node_path}/{children_list[0]}"
        remove_node_listener(lock_path, cb, name)


def add_node_listener(node_path: str, cb: Callable, name="default"):
    if name not in ZkClient.zk_client_dict:
        raise Exception(
            f"{name} zk client not exist, can't add node listener:{node_path}"
        )
    if cb not in __node_listener[name][node_path]:
        __node_listener[name][node_path].append(cb)
    if node_path not in __watched_node_list[name]:
        __watched_node_list[name].append(node_path)
        ZkClient.zk_client_dict[name].get(
            node_path, watch=partial(__watch_node_event, node_path, name)
        )


def remove_node_listener(node_path: str, cb: Callable = None, name="default"):
    if name not in ZkClient.zk_client_dict:
        raise Exception(
            f"{name} zk client not exist, can't remove node listener:{node_path}"
        )
    if cb is None:
        __watched_node_list[name].remove(node_path)
        __node_listener[name][node_path].clear
    else:
        __node_listener[name][node_path].remove(cb)
        if len(__node_listener[name][node_path]) == 0:
            __watched_node_list[name].remove(node_path)


def add_children_listener(node_path: str, cb: Callable = None,name="default"):
    if name not in ZkClient.zk_client_dict:
        raise Exception(
            f"{name} zk client not exist, can't add children listener:{node_path}"
        )

    if cb not in __children_listener[name][node_path]:
        __children_listener[name][node_path].append(cb)

    if node_path not in __watched_children_list[name]:
        __watched_children_list[name].append(node_path)
        ZkClient.zk_client_dict[name].get_children(
            node_path, partial(__watch_children_event, node_path, name)
        )


def remove_children_listener(node_path: str, cb: Callable = None, name="default"):
    if name not in ZkClient.zk_client_dict:
        raise Exception(
            f"{name} zk client not exist, can't remove children listener:{node_path}"
        )
    if cb is None:
        __watched_children_list[name].remove(node_path)
        __children_listener[name][node_path].clear()
    else:
        __children_listener[name][node_path].remove(cb)
        if len(__children_listener[name][node_path]) == 0:
            __watch_children_event[name].remove(node_path)


# util
__symbol = "123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"  # 35


def __get_unlock_pool_symbol(lock_symbol: list, pool_symbol: list):
    for s in pool_symbol:
        if s not in lock_symbol:
            return s
    return None


def __get_unused_pool_symbol(used_symbol: list, max_len: int):
    if max_len > 35:
        return Exception("pool size must len")
    all_symbol = __symbol[:max_len]
    for s in all_symbol[::-1]:
        if s not in used_symbol:
            return s
    raise Exception("all symbol are used")


if __name__ == "__main__":
    # s = __get_unused_pool_symbol([], 6)
    # print(s)
    pass
