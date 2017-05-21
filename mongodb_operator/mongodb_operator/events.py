import logging
from time import sleep

from kubernetes import watch

from .mongodb_tpr_v1alpha1_api import MongoDBThirdPartyResourceV1Alpha1Api
from .kubernetes_helpers import (create_service, delete_service,
                                 create_statefulset, reap_deployment)


def event_listener(shutting_down, timeout_seconds):
    logging.info('thread started')
    mongodb_tpr_api = MongoDBThirdPartyResourceV1Alpha1Api()
    event_watch = watch.Watch()
    while not shutting_down.isSet():
        try:
            for event in event_watch.stream(
                    mongodb_tpr_api.list_mongodb_for_all_namespaces,
                    timeout_seconds=timeout_seconds):

                event_switch(event)
        except Exception as e:
            # Last resort: catch all exceptions to keep the thread alive
            logging.exception(e)
            sleep(int(timeout_seconds))
    else:
        event_watch.stop()
        logging.info('thread stopped')


def event_switch(event):
    if 'type' not in event and 'object' not in event:
        # We can't work with that event
        logging.warning('malformed event: {}'.format(event))
        return

    event_type = event['type']
    cluster_object = event['object']

    if event_type == 'ADDED':
        add(cluster_object)
    elif event_type == 'MODIFIED':
        modify(cluster_object)
    elif event_type == 'DELETED':
        delete(cluster_object)


def add(cluster_object):
    # Create service
    create_service(cluster_object)

    # Create deployment
    create_statefulset(cluster_object)


def modify(cluster_object):
    logging.warning('UPDATE NOT IMPLEMENTED YET')


def delete(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    # Delete service
    delete_service(name, namespace)

    # Gracefully delete deployment, replicaset and pods
    reap_deployment(name, namespace)
