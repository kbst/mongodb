import logging
from time import sleep

from kubernetes import client

from .kubernetes_resources import (get_default_label_selector,
                                   get_service_object, get_statefulset_object)


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
        statefulset = appsv1beta1api.create_namespaced_stateful_set(namespace, body)
    except client.rest.ApiException as e:
        if e.status == 409:
            # Deployment already exists
            logging.debug('deploy/{} in ns/{} already exists'.format(
                name, namespace))
        else:
            logging.exception(e)
        return False
    else:
        logging.info('created deploy/{} in ns/{}'.format(name, namespace))
        return statefulset


def update_statefulset(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    appsv1beta1api = client.AppsV1beta1Api()
    body = get_statefulset_object(cluster_object)
    try:
        statefulset = v1beta1api.patch_namespaced_stateful_set(
            name, namespace, body)
    except client.rest.ApiException as e:
        logging.exception(e)
        return False
    else:
        logging.info('updated deploy/{} in ns/{}'.format(name, namespace))
        return statefulset


def delete_deployment(name, namespace, delete_options=None):
    v1beta1api = client.ExtensionsV1beta1Api()
    if not delete_options:
        delete_options = client.V1DeleteOptions()
    try:
        v1beta1api.delete_namespaced_deployment(
            name, namespace, delete_options, orphan_dependents=False)
    except client.rest.ApiException as e:
        if e.status == 404:
            # Deployment does not exist, nothing to delete but
            # we can consider this a success.
            logging.debug(
                'not deleting nonexistent deploy/{} from ns/{}'.format(
                    name, namespace))
            return True
        else:
            logging.exception(e)
            return False
    else:
        logging.info('deleted deploy/{} from ns/{}'.format(
            name, namespace))
        return True


def reap_deployment(name, namespace):
    """
    Replicate reaping logic from kubectl
    https://goo.gl/hzOJoU
    """
    v1beta1api = client.ExtensionsV1beta1Api()

    # First pause and scale down deployment
    body = {'spec': {
                'replicas': 0,
                'revision_history_limit': 0,
                'paused': True}}
    try:
        v1beta1api.patch_namespaced_deployment(
            name, namespace, body)
    except client.rest.ApiException as e:
        if e.status == 404:
            # Deployment does not exist, nothing to gracefully delete
            msg = 'can not gracefully delete nonexistent deploy/{} from ns/{}'
            logging.debug(msg.format(name, namespace))
        else:
            # Unexpected exception, stop reaping
            logging.exception(e)
            return False

    # Find related replicaset[s]
    label_selector = get_default_label_selector(name=name)
    replica_sets = v1beta1api.list_namespaced_replica_set(
        namespace,
        label_selector=label_selector)

    num_related = len(replica_sets.items)
    if num_related != 1:
        # If we found != 1 replica set here, the deployment controller is
        # probably doing its job right now. Let's not get involved.
        # We will retry in the next garbage collection run.
        msg = 'found {} replicasets. Refusing to reap deploy/{} from ns/{}'
        logging.warning(msg.format(num_related, name, namespace))
        return False

    rs_name = replica_sets.items[0].metadata.name
    # Gracefully wait until scaled down,
    # finally delete replicaset and deployment
    replicaset_deleted = False
    deployment_deleted = False
    for i in range(5):
        # A little back-off before the next try
        sleep(i * 2)

        try:
            replica_set = v1beta1api.read_namespaced_replica_set(
                rs_name, namespace)
        except client.rest.ApiException as e:
            if e.status == 404:
                # Replicaset does not exist, nothing to delete
                # proceed with deployment
                replicaset_deleted = True
            else:
                logging.exception(e)
                return False
        else:
            if replica_set.status.replicas == 0:
                # Delete the replicaset
                replicaset_deleted = delete_replica_set(rs_name, namespace)

        # Delete the deployment only after the replicaset was deleted.
        # If the replicaset isn't deleted properly keeping deployment
        # allows the next garbage collection run to retry this
        if replicaset_deleted:
            deployment_deleted = delete_deployment(name, namespace)

        if replicaset_deleted and deployment_deleted:
            return True
    # If none of the retries succeeded
    return False


def delete_replica_set(name, namespace, delete_options=None):
    v1beta1api = client.ExtensionsV1beta1Api()
    if not delete_options:
        delete_options = client.V1DeleteOptions()
    try:
        v1beta1api.delete_namespaced_replica_set(
            name,
            namespace,
            delete_options,
            orphan_dependents=False)
    except client.rest.ApiException as e:
        if e.status == 404:
            # ReplicaSet does not exist, nothing to delete but
            # we can consider this a success.
            logging.debug(
                'not deleting nonexistent rs/{} from ns/{}'.format(
                    name, namespace))
            return True
        else:
            logging.exception(e)
        return False
    else:
        logging.info('deleted rs/{} from ns/{}'.format(
            name, namespace))
        return True
