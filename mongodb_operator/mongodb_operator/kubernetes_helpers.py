import logging, json, random, string
from time import sleep
from tempfile import NamedTemporaryFile
from base64 import b64decode

import delegator
from kubernetes import client

from .kubernetes_resources import (get_default_label_selector,
                                   get_service_object, get_statefulset_object,
                                   get_secret_object)


def get_random_password(length=23):
    chars = string.ascii_letters + string.digits + '!@#$%^&*()'
    rnd = random.SystemRandom()
    return ''.join(rnd.choice(chars) for i in range(length))


def create_admin_secret(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    v1 = client.CoreV1Api()
    body = get_secret_object(
        cluster_object,
        '-admin-credentials',
        {'username': 'root', 'password': get_random_password()})
    try:
        secret = v1.create_namespaced_secret(namespace, body)
    except client.rest.ApiException as e:
        if e.status == 409:
            # Secret already exists
            logging.debug('secret/{}-admin-credentials in ns/{} already exists'.format(
                name, namespace))
        else:
            logging.exception(e)
        return False
    else:
        logging.info('created secret/{}-admin-credentials in ns/{}'.format(name, namespace))
        return secret


def create_monitoring_secret(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    v1 = client.CoreV1Api()
    body = get_secret_object(
        cluster_object,
        '-monitoring-credentials',
        {'username': 'monitoring', 'password': get_random_password()})
    try:
        secret = v1.create_namespaced_secret(namespace, body)
    except client.rest.ApiException as e:
        if e.status == 409:
            # Secret already exists
            logging.debug('secret/{}-monitoring-credentials in ns/{} already exists'.format(
                name, namespace))
        else:
            logging.exception(e)
        return False
    else:
        logging.info('created secret/{}-monitoring-credentials in ns/{}'.format(name, namespace))
        return secret


def get_certificate_authority(name, namespace, suffix='svc.cluster.local'):
    common_name = '{}.{}.{}'.format(name, namespace, suffix)
    ca_csr = {
    	'CN': common_name,
    	'key': {
    		'algo': 'rsa',
    		'size': 2048
    	},
    	'names': [{
    		'O': common_name
    	}]}

    with NamedTemporaryFile() as ca_csr_file:
        ca_csr_json = json.dumps(ca_csr).encode('utf-8')
        ca_csr_file.write(ca_csr_json + b'\n')
        ca_csr_file.flush()
        cmd = './cfssl genkey -initca {}'.format(ca_csr_file.name)
        c = delegator.run(cmd)

    if not c.out:
        logging.error('cfssl {}'.format(c.err))

    r = json.loads(c.out)
    return r['cert'], r['key'], r['csr']


def create_certificate_authority_secret(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    v1 = client.CoreV1Api()
    cert_pem, key_pem, csr_pem = get_certificate_authority(name, namespace)
    body = get_secret_object(
        cluster_object,
        '-ca',
        {'ca.pem': cert_pem, 'ca-key.pem': key_pem})
    try:
        secret = v1.create_namespaced_secret(namespace, body)
    except client.rest.ApiException as e:
        if e.status == 409:
            # Secret already exists
            logging.debug('secret/{}-ca in ns/{} already exists'.format(
                name, namespace))
        else:
            logging.exception(e)
        return False
    else:
        logging.info('created secret/{}-ca in ns/{}'.format(name, namespace))
        return secret


def read_certificate_authority_secret(cluster_object):
    name = '{}-ca'.format(cluster_object['metadata']['name'])
    namespace = cluster_object['metadata']['namespace']
    v1 = client.CoreV1Api()
    try:
        secret = v1.read_namespaced_secret(name, namespace)
    except client.rest.ApiException as e:
        if e.status == 404:
            # Secret already exists
            logging.debug('secret/{} in ns/{} does not exist'.format(
                name, namespace))
        else:
            logging.exception(e)
        return False
    else:
        return secret


def get_client_certificate(name, namespace, ca_pem, ca_key_pem):
    common_name = '{}-client'.format(name)
    client_csr = {
    	'CN': common_name,
    	'hosts': [],
    	'key': {
    		'algo': 'rsa',
    		'size': 2048
    	},
    	'names': [{
    		'O': common_name
    	}]
    }

    ca_file = NamedTemporaryFile(delete=False)
    ca_key_file = NamedTemporaryFile(delete=False)
    client_csr_file = NamedTemporaryFile(delete=False)

    ca_file.write(ca_pem)
    ca_file.flush()
    ca_key_file.write(ca_key_pem)
    ca_key_file.flush()
    client_csr_json = json.dumps(client_csr).encode('utf-8')
    client_csr_file.write(client_csr_json + b'\n')
    client_csr_file.flush()

    cmd = '''./cfssl gencert \
             -ca={} \
             -ca-key={} \
             -config=ca-config.json \
             -profile=client {}'''.format(
                ca_file.name,
                ca_key_file.name,
                client_csr_file.name)

    c = delegator.run(cmd)

    ca_file.close()
    ca_key_file.close()
    client_csr_file.close()

    if not c.out:
        logging.error('cfssl {}'.format(c.err))

    r = json.loads(c.out)

    mongod_pem =  r['cert'] + r['key']
    return mongod_pem, r['csr']


def create_client_certificate_secret(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    v1 = client.CoreV1Api()
    ca_secret = read_certificate_authority_secret(cluster_object)
    ca_pem = b64decode(ca_secret.data['ca.pem'])
    ca_key_pem = b64decode(ca_secret.data['ca-key.pem'])
    mongod_pem, csr_pem = get_client_certificate(
        name, namespace, ca_pem, ca_key_pem)
    body = get_secret_object(
        cluster_object,
        '-client-certificate',
        {'mongod.pem': mongod_pem, 'ca.pem': ca_pem.decode('utf-8')})
    try:
        secret = v1.create_namespaced_secret(namespace, body)
    except client.rest.ApiException as e:
        if e.status == 409:
            # Secret already exists
            logging.debug(
                'secret/{}-client-certificate in ns/{} already exists'.format(
                    name, namespace))
        else:
            logging.exception(e)
        return False
    else:
        logging.info('created secret/{}-client-certificate in ns/{}'.format(
            name, namespace))
        return secret


def read_client_certificate_secret(cluster_object):
    name = '{}-client-certificate'.format(cluster_object['metadata']['name'])
    namespace = cluster_object['metadata']['namespace']
    v1 = client.CoreV1Api()
    try:
        secret = v1.read_namespaced_secret(name, namespace)
    except client.rest.ApiException as e:
        if e.status == 404:
            # Secret already exists
            logging.debug('secret/{} in ns/{} does not exist'.format(
                name, namespace))
        else:
            logging.exception(e)
        return False
    else:
        return secret


def create_service(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    v1 = client.CoreV1Api()
    body = get_service_object(cluster_object)
    try:
        service = v1.create_namespaced_service(namespace, body)
    except client.rest.ApiException as e:
        if e.status == 409:
            # Service already exists
            logging.debug('svc/{} in ns/{} already exists'.format(
                name, namespace))
        else:
            logging.exception(e)
        return False
    else:
        logging.info('created svc/{} in ns/{}'.format(name, namespace))
        return service


def update_service(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    v1 = client.CoreV1Api()
    body = get_service_object(cluster_object)
    try:
        service = v1.patch_namespaced_service(name, namespace, body)
    except client.rest.ApiException as e:
        logging.exception(e)
        return False
    else:
        logging.info('updated svc/{} in ns/{}'.format(name, namespace))
        return service


def delete_service(name, namespace):
    v1 = client.CoreV1Api()
    try:
        v1.delete_namespaced_service(name, namespace)
    except client.rest.ApiException as e:
        logging.exception(e)
        return False
    else:
        logging.info('deleted svc/{} from ns/{}'.format(name, namespace))
        return True


def create_statefulset(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    appsv1beta1api = client.AppsV1beta1Api()
    body = get_statefulset_object(cluster_object)
    try:
        statefulset = appsv1beta1api.create_namespaced_stateful_set(
            namespace, body)
    except client.rest.ApiException as e:
        if e.status == 409:
            # Deployment already exists
            logging.debug('statefulset/{} in ns/{} already exists'.format(
                name, namespace))
        else:
            logging.exception(e)
        return False
    else:
        logging.info('created statefulset/{} in ns/{}'.format(name, namespace))
        return statefulset


def update_statefulset(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    appsv1beta1api = client.AppsV1beta1Api()
    body = get_statefulset_object(cluster_object)
    try:
        statefulset = appsv1beta1api.patch_namespaced_stateful_set(
            name, namespace, body)
    except client.rest.ApiException as e:
        logging.exception(e)
        return False
    else:
        logging.info('updated statefulset/{} in ns/{}'.format(name, namespace))
        return statefulset


def delete_statefulset(name, namespace, delete_options=None):
    appsv1beta1api = client.AppsV1beta1Api()
    if not delete_options:
        delete_options = client.V1DeleteOptions()
    try:
        appsv1beta1api.delete_namespaced_stateful_set(
            name, namespace, delete_options, orphan_dependents=False)
    except client.rest.ApiException as e:
        if e.status == 404:
            # StatefulSet does not exist, nothing to delete but
            # we can consider this a success.
            logging.debug(
                'not deleting nonexistent statefulset/{} from ns/{}'.format(
                    name, namespace))
            return True
        else:
            logging.exception(e)
            return False
    else:
        logging.info('deleted statefulset/{} from ns/{}'.format(
            name, namespace))
        return True


def reap_statefulset(name, namespace):
    appsv1beta1api = client.AppsV1beta1Api()

    # First pause and scale down statefulset
    body = {'spec': {
                'replicas': 0,
                'revision_history_limit': 0,
                'paused': True}}
    try:
        appsv1beta1api.patch_namespaced_stateful_set(
            name, namespace, body)
    except client.rest.ApiException as e:
        if e.status == 404:
            # StatefulSet does not exist, nothing to gracefully delete
            msg = 'can not gracefully delete nonexistent statefulset/{} from ns/{}'
            logging.debug(msg.format(name, namespace))
            return True
        else:
            # Unexpected exception, stop reaping
            logging.exception(e)
            return False

    # Gracefully wait until scaled down, finally delete StatefulSet
    statefulset_deleted = False
    for i in range(5):
        # A little back-off before the next try
        sleep(i * 2)

        try:
            statefulset = appsv1beta1api.read_namespaced_stateful_set(
                name, namespace)
        except client.rest.ApiException as e:
            if e.status == 404:
                # StatefulSet does not exist, nothing to delete
                return True
            else:
                logging.exception(e)
                return False
        else:
            if statefulset.status.replicas == 0:
                # Delete the statefulset
                statefulset_deleted = delete_statefulset(name, namespace)

        if statefulset_deleted:
            return True
    # If none of the retries succeeded
    return False
