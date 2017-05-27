from base64 import b64decode
from tempfile import NamedTemporaryFile

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import delegator

from .kubernetes_helpers import read_secret

def get_member_hostname(member_id, cluster_name, namespace, dns_suffix):
    return '{}-{}.{}.{}.{}'.format(
        cluster_name, member_id, cluster_name, namespace, dns_suffix)

def initiate_replicaset(cluster_object, dns_suffix='svc.cluster.local'):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    try:
        replicas = cluster_object['spec']['mongodb']['replicas']
    except KeyError:
        replicas = 3

    _rs_config={
        '_id': name,
        'version': 1,
        'members': []
    }

    for _id in range(replicas):
        _member_hostname = get_member_hostname(_id, name, namespace, dns_suffix)
        _rs_config['members'].append({
            '_id': _id,
            'host': _member_hostname})

    client_certificate_secret = read_secret(
        '{}-client-certificate'.format(name), namespace)

    ca_pem_file = NamedTemporaryFile()
    ca_pem_file.write(
        b64decode(client_certificate_secret.data['ca.pem']))
    ca_pem_file.flush()

    mongod_pem_file = NamedTemporaryFile()
    mongod_pem_file.write(
        b64decode(client_certificate_secret.data['mongod.pem']))
    mongod_pem_file.flush()

    # MongoDB rs.initiate requires localhost connection
    # we use kubectl port-forward to achieve this
    pod_name = '{}-0'.format(name)
    cmd = 'kubectl -n {} port-forward {} {}:27017'.format(
        namespace, pod_name, 27017)
    port_forward = delegator.run(cmd, block=False)

    mc = MongoClient(
        'mongodb://localhost:27017',
        ssl=True,
        ssl_ca_certs=ca_pem_file.name,
        ssl_certfile=mongod_pem_file.name)
    try:
        mc.admin.command('ismaster')
    except ConnectionFailure:
        mc.admin.command("replSetInitiate", _rs_config)

    ca_pem_file.close()
    mongod_pem_file.close()
    port_forward.kill()
