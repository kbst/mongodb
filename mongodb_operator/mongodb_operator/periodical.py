import logging
from time import sleep

from kubernetes import client

from .kubernetes_resources import get_default_label_selector
from .kubernetes_helpers import (list_cluster_mongodb_object,
                                 get_namespaced_mongodb_object,
                                 create_service, update_service,
                                 delete_service, create_statefulset,
                                 update_statefulset, reap_statefulset,
                                 delete_secret)
from .mongodb_helpers import check_if_replicaset_needs_setup


def periodical_check(shutting_down, sleep_seconds):
    logging.info('thread started')
    while not shutting_down.isSet():
        try:
            # First make sure all expected resources exist
            check_existing()

            # Then garbage collect resources from deleted clusters
            collect_garbage()
        except Exception as e:
            # Last resort: catch all exceptions to keep the thread alive
            logging.exception(e)
        finally:
            sleep(int(sleep_seconds))
    else:
        logging.info('thread stopped')


VERSION_CACHE = {}


def is_version_cached(resource):
    uid = resource.metadata.uid
    version = resource.metadata.resource_version

    if uid in VERSION_CACHE and VERSION_CACHE[uid] == version:
        return True

    return False


def cache_version(resource):
    uid = resource.metadata.uid
    version = resource.metadata.resource_version

    VERSION_CACHE[uid] = version


def check_existing():
    try:
        cluster_list = list_cluster_mongodb_object()
    except client.rest.ApiException as e:
        # If for any reason, k8s api gives us an error here, there is
        # nothing for us to do but retry later
        logging.exception(e)
        return False

    core_api = client.CoreV1Api()
    apps_api = client.AppsV1beta2Api()
    for cluster_object in cluster_list['items']:
        name = cluster_object['metadata']['name']
        namespace = cluster_object['metadata']['namespace']

        # Check service exists
        try:
            service = core_api.read_namespaced_service(name, namespace)
        except client.rest.ApiException as e:
            if e.status == 404:
                # Create missing service
                created_service = create_service(cluster_object)
                if created_service:
                    # Store latest version in cache
                    cache_version(created_service)
            else:
                logging.exception(e)
        else:
            if not is_version_cached(service):
                # Update since we don't know if it's configured correctly
                updated_service = update_service(cluster_object)
                if updated_service:
                    # Store latest version in cache
                    cache_version(updated_service)

        # Check statefulset exists
        try:
            statefulset = apps_api.read_namespaced_stateful_set(
                name, namespace)
        except client.rest.ApiException as e:
            if e.status == 404:
                # Create missing statefulset
                created_statefulset = create_statefulset(cluster_object)
                if created_statefulset:
                    # Store latest version in cache
                    cache_version(created_statefulset)
            else:
                logging.exception(e)
        else:
            if not is_version_cached(statefulset):
                # Update since we don't know if it's configured correctly
                updated_statefulset = update_statefulset(cluster_object)
                if updated_statefulset:
                    # Store latest version in cache
                    cache_version(updated_statefulset)

        # Check replica set status
        check_if_replicaset_needs_setup(cluster_object)


def collect_garbage():
    core_api = client.CoreV1Api()
    apps_api = client.AppsV1beta2Api()
    label_selector = get_default_label_selector()

    # Find all services that match our labels
    try:
        service_list = core_api.list_service_for_all_namespaces(
            label_selector=label_selector)
    except client.rest.ApiException as e:
        logging.exception(e)
    else:
        # Check if service belongs to an existing cluster
        for service in service_list.items:
            name = service.metadata.name
            namespace = service.metadata.namespace

            try:
                get_namespaced_mongodb_object(name, namespace)
            except client.rest.ApiException as e:
                if e.status == 404:
                    # Delete service
                    delete_service(name, namespace)
                else:
                    logging.exception(e)

    # Find all statefulsets that match our labels
    try:
        statefulset_list = apps_api.list_stateful_set_for_all_namespaces(
            label_selector=label_selector)
    except client.rest.ApiException as e:
        logging.exception(e)
    else:
        # Check if statefulsets belongs to an existing cluster
        for statefulset in statefulset_list.items:
            name = statefulset.metadata.name
            namespace = statefulset.metadata.namespace

            try:
                get_namespaced_mongodb_object(name, namespace)
            except client.rest.ApiException as e:
                if e.status == 404:
                    # Gracefully delete statefulsets and pods
                    reap_statefulset(name, namespace)
                else:
                    logging.exception(e)

    # Find all secrets that match our labels
    try:
        secret_list = core_api.list_secret_for_all_namespaces(
            label_selector=label_selector)
    except client.rest.ApiException as e:
        logging.exception(e)
    else:
        # Check if secrets belongs to an existing cluster
        for secret in secret_list.items:
            cluster_name = secret.metadata.labels['cluster']
            secret_name = secret.metadata.name
            namespace = secret.metadata.namespace

            try:
                get_namespaced_mongodb_object(
                    cluster_name, namespace)
            except client.rest.ApiException as e:
                if e.status == 404:
                    # Delete service
                    delete_secret(secret_name, namespace)
                else:
                    logging.exception(e)
