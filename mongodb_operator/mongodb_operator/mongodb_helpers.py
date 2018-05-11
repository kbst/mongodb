import logging
import json
from base64 import b64decode

from kubernetes.client.apis import core_v1_api
from kubernetes.stream import stream

from .kubernetes_helpers import read_secret


DNS_SUFFIX = 'svc.cluster.local'


def get_member_hostname(member_id, cluster_name, namespace, dns_suffix):
    return '{}-{}.{}.{}.{}'.format(
        cluster_name, member_id, cluster_name, namespace, dns_suffix)


def check_if_replicaset_needs_setup(cluster_object, dns_suffix=DNS_SUFFIX):
    core_api = core_v1_api.CoreV1Api()
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']

    pod_name = '{}-0'.format(name)
    exec_cmd = [
        'mongo',
        'localhost:27017/admin',
        '--ssl',
        '--sslCAFile', '/etc/ssl/mongod/ca.pem',
        '--sslPEMKeyFile', '/etc/ssl/mongod/mongod.pem',
        '--eval', 'rs.status()']
    exec_resp = stream(core_api.connect_get_namespaced_pod_exec,
        pod_name,
        namespace,
        command=exec_cmd,
        container='mongod',
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False)

    # If the replica set is not initialized yet, we initialize it
    if '"ok" : 0' in exec_resp and \
       '"codeName" : "NotYetInitialized"' in exec_resp:
        initiate_replicaset(cluster_object, dns_suffix=dns_suffix)

    # If we can get the replica set status without authenticating as the
    # admin user first, we have to create the users
    if '"ok" : 1' in exec_resp:
        create_users(cluster_object)


def initiate_replicaset(cluster_object, dns_suffix=DNS_SUFFIX):
    core_api = core_v1_api.CoreV1Api()
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    try:
        replicas = cluster_object['spec']['mongodb']['replicas']
    except KeyError:
        replicas = 3

    _rs_config = {
        '_id': name,
        'version': 1,
        'members': []
    }

    for _id in range(replicas):
        _member_hostname = get_member_hostname(
            _id, name, namespace, dns_suffix)
        _rs_config['members'].append({
            '_id': _id,
            'host': _member_hostname})

    pod_name = '{}-0'.format(name)
    exec_cmd = [
        'mongo',
        'localhost:27017/admin',
        '--ssl',
        '--sslCAFile', '/etc/ssl/mongod/ca.pem',
        '--sslPEMKeyFile', '/etc/ssl/mongod/mongod.pem',
        '--eval', 'rs.initiate({})'.format(json.dumps(_rs_config))]
    exec_resp = stream(core_api.connect_get_namespaced_pod_exec,
        pod_name,
        namespace,
        command=exec_cmd,
        container='mongod',
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False)

    if '{ "ok" : 1 }' in exec_resp:
        logging.info('initialized replicaset {} in ns/{}'.format(
            name, namespace))
    elif '"ok" : 0' in exec_resp and \
         '"codeName" : "NodeNotFound"' in exec_resp:
        logging.info('waiting for {} {} replicaset members in ns/{}'.format(
            replicas, name, namespace))
        logging.debug(exec_resp)
    else:
        logging.error('error initializing replicaset {} in ns/{}\n{}'.format(
            name, namespace, exec_resp))


def create_users(cluster_object):
    core_api = core_v1_api.CoreV1Api()
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    try:
        replicas = cluster_object['spec']['mongodb']['replicas']
    except KeyError:
        replicas = 3

    admin_credentials = read_secret(
        '{}-admin-credentials'.format(name), namespace)
    admin_username = b64decode(
        admin_credentials.data['username']).decode('utf-8')
    admin_password = b64decode(
        admin_credentials.data['password']).decode('utf-8')

    monitoring_credentials = read_secret(
        '{}-monitoring-credentials'.format(name), namespace)
    monitoring_username = b64decode(
        monitoring_credentials.data['username']).decode('utf-8')
    monitoring_password = b64decode(
        monitoring_credentials.data['password']).decode('utf-8')

    mongo_command = '''
        admin = db.getSiblingDB("admin")
        admin.createUser(
          {{
            user: "{}",
            pwd: "{}",
            roles: [ {{ role: "root", db: "admin" }} ]
          }}
        )
        admin.auth(
          "{}",
          "{}"
        )
        admin.createUser(
          {{
            user: "{}",
            pwd: "{}",
            roles: [ {{ role: "clusterMonitor", db: "admin" }} ]
          }}
        )
    '''.format(
        admin_username, admin_password,
        admin_username, admin_password,
        monitoring_username, monitoring_password)

    for i in range(replicas):
        pod_name = '{}-{}'.format(name, i)
        exec_cmd = [
            'mongo',
            'localhost:27017/admin',
            '--ssl',
            '--sslCAFile', '/etc/ssl/mongod/ca.pem',
            '--sslPEMKeyFile', '/etc/ssl/mongod/mongod.pem',
            '--eval', '{}'.format(mongo_command)]
        exec_resp = stream(core_api.connect_get_namespaced_pod_exec,
            pod_name,
            namespace,
            command=exec_cmd,
            container='mongod',
            stderr=True,
            stdin=False,
            stdout=True,
            tty=False)

        if 'Successfully added user: {' in exec_resp:
            logging.info('created users for {} in ns/{}'.format(
                name, namespace))
            return True
        elif "Error: couldn't add user: not master :" in exec_resp:
            # most of the time member-0 is elected master
            # if it is not we get this error and need to
            # loop through members until we find the master
            continue
        else:
            logging.error('error creating users for {} in ns/{}\n{}'.format(
                name, namespace, exec_resp))
        return False
