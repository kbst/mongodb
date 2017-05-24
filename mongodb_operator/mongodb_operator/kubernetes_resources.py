import json

from kubernetes import client


def get_default_labels(name=None):
    default_labels = {
        'operated-by': 'mongodb.operator.kubestack.com',
        'heritage': 'kubestack.com'}
    if name:
        default_labels['cluster'] = name
    return default_labels


def get_default_label_selector(name=None):
    default_labels = get_default_labels(name=name)
    default_label_selectors = []
    for label in default_labels:
        default_label_selectors.append(
            '{}={}'.format(label, default_labels[label]))
    return ','.join(default_label_selectors)


def get_service_object(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']
    service = client.V1Service()

    # Metadata
    service.metadata = client.V1ObjectMeta(
        name=name,
        namespace=namespace,
        labels=get_default_labels(name=name))
    # Add the monitoring label so that metrics get picked up by Prometheus
    service.metadata.labels['monitoring.kubestack.com'] = 'metrics'

    # Spec
    mongodb_port = client.V1ServicePort(
        name='mongod', port=27017, protocol='TCP')
    metrics_port = client.V1ServicePort(
        name='metrics', port=9001, protocol='TCP')

    service.spec = client.V1ServiceSpec(
        cluster_ip='None',
        selector=get_default_labels(name=name),
        ports=[mongodb_port, metrics_port])
    return service


def get_statefulset_object(cluster_object):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']

    try:
        replicas = cluster_object['spec']['mongodb']['replicas']
    except KeyError:
        replicas = 3

    try:
        mongodb_limit_cpu = \
            cluster_object['spec']['mongodb']['mongodb_limit_cpu']
    except KeyError:
        mongodb_limit_cpu = '100m'

    try:
        mongodb_limit_memory = \
            cluster_object['spec']['mongodb']['mongodb_limit_memory']
    except KeyError:
        mongodb_limit_memory = '64Mi'

    statefulset = client.V1beta1StatefulSet()

    # Metadata
    statefulset.metadata = client.V1ObjectMeta(
        name=name,
        namespace=namespace,
        labels=get_default_labels(name=name))

    # Spec
    statefulset.spec = client.V1beta1StatefulSetSpec(
        replicas=replicas,
        service_name=name)

    statefulset.spec.template = client.V1PodTemplateSpec()
    statefulset.spec.template.metadata = client.V1ObjectMeta(
        labels=get_default_labels(name=name))

    statefulset.spec.template.spec = client.V1PodSpec()
    statefulset.spec.template.spec.affinity = client.V1Affinity(
        pod_anti_affinity=client.V1PodAntiAffinity(
            required_during_scheduling_ignored_during_execution=[
                client.V1PodAffinityTerm(
                    topology_key='kubernetes.io/hostname',
                    label_selector=client.V1LabelSelector(
                        match_expressions=[client.V1LabelSelectorRequirement(
                            key='cluster',
                            operator='In',
                            values=[name])]))]))
    # MongoDB container
    mongodb_port = client.V1ContainerPort(
        name='mongodb', container_port=27017, protocol='TCP')
    mongodb_tls_volumemount = client.V1VolumeMount(
        name='mongo-tls',
        read_only=True,
        mount_path='/etc/ssl/mongod')
    mongodb_data_volumemount = client.V1VolumeMount(
        name='mongo-data', read_only=False, mount_path='/data/db')
    mongodb_resources = client.V1ResourceRequirements(
        limits={
            'cpu': mongodb_limit_cpu, 'memory': mongodb_limit_memory},
        requests={
            'cpu': mongodb_limit_cpu, 'memory': mongodb_limit_memory})
    mongodb_container = client.V1Container(
        name='mongod',
        command=[
            'mongod',
            '--auth',
            '--replSet', name,
            '--sslMode', 'requireSSL',
            '--clusterAuthMode', 'x509',
            '--sslPEMKeyFile', '/etc/ssl/mongod/mongod.pem',
            '--sslCAFile', '/etc/ssl/mongod/ca.pem'],
        image='mongo:3.4.1',
        ports=[mongodb_port],
        volume_mounts=[mongodb_tls_volumemount, mongodb_data_volumemount],
        resources=mongodb_resources)

    # Metrics container
    metrics_port = client.V1ContainerPort(
        name='metrics', container_port=9001, protocol='TCP')
    metrics_resources = client.V1ResourceRequirements(
        limits={'cpu': '50m', 'memory': '16Mi'},
        requests={'cpu': '50m', 'memory': '16Mi'})
    metrics_secret_name = '{}-monitoring-credentials'.format(name)
    metrics_username_env_var = client.V1EnvVar(
        name='MONGODB_MONITORING_USERNAME',
        value_from=client.V1EnvVarSource(
            secret_key_ref=client.V1SecretKeySelector(
                name=metrics_secret_name,
                key='username')))
    metrics_password_env_var = client.V1EnvVar(
        name='MONGODB_MONITORING_PASSWORD',
        value_from=client.V1EnvVarSource(
            secret_key_ref=client.V1SecretKeySelector(
                name=metrics_secret_name,
                key='password')))
    metrics_container = client.V1Container(
        name='prometheus-exporter',
        image='quay.io/kubestack/prometheus-mongodb-exporter:latest',
        command=[
            '/bin/sh',
            '-c',
            '/bin/mongodb_exporter --mongodb.uri mongodb://${MONGODB_MONITORING_USERNAME}:${MONGODB_MONITORING_PASSWORD}@127.0.0.1:27017/admin --mongodb.tls-cert /etc/ssl/mongod/mongod.pem --mongodb.tls-ca /etc/ssl/mongod/ca.pem'],
        ports=[metrics_port],
        resources=metrics_resources,
        volume_mounts=[mongodb_tls_volumemount],
        env=[metrics_username_env_var, metrics_password_env_var])

    statefulset.spec.template.spec.containers = [
        mongodb_container, metrics_container]

    ca_volume = client.V1Volume(
        name='mongo-ca',
        secret=client.V1SecretVolumeSource(
            secret_name='{}-ca'.format(name),
            items=[
                client.V1KeyToPath(
                    key='ca.pem',
                    path='ca.pem'),
                client.V1KeyToPath(
                    key='ca-key.pem',
                    path='ca-key.pem')]))
    tls_volume = client.V1Volume(
        name='mongo-tls',
        empty_dir=client.V1EmptyDirVolumeSource())
    data_volume = client.V1Volume(
        name='mongo-data',
        empty_dir=client.V1EmptyDirVolumeSource())
    statefulset.spec.template.spec.volumes = [
        ca_volume, tls_volume, data_volume]

    # Init container
    # For now use annotation format for init_container to support K8s >= 1.5
    statefulset.spec.template.metadata.annotations = {'pod.beta.kubernetes.io/init-containers': '[{"name": "cert-init","image": "quay.io/kubestack/mongodb-init:latest","volumeMounts": [{"readOnly": true,"mountPath": "/etc/ssl/mongod-ca","name": "mongo-ca"}, {"mountPath": "/etc/ssl/mongod","name": "mongo-tls"}],"env": [{"name": "METADATA_NAME","valueFrom": {"fieldRef": {"apiVersion": "v1","fieldPath": "metadata.name"}}}, {"name": "NAMESPACE","valueFrom": {"fieldRef": {"apiVersion": "v1","fieldPath": "metadata.namespace"}}}],"command": ["ansible-playbook","member-cert.yml"],"imagePullPolicy": "Always"}]'}

    #tls_init_ca_volumemount = client.V1VolumeMount(
    #    name='mongo-ca',
    #    read_only=True,
    #    mount_path='/etc/ssl/mongod-ca')
    #tls_init_container = client.V1Container(
    #    name="cert-init",
    #    image="quay.io/kubestack/mongodb-init:latest",
    #    volume_mounts=[tls_init_ca_volumemount, mongodb_tls_volumemount],
    #    env=[
    #        client.V1EnvVar(
    #            name='METADATA_NAME',
    #            value_from=client.V1EnvVarSource(
    #                field_ref=client.V1ObjectFieldSelector(
    #                    api_version='v1',
    #                    field_path='metadata.name'))),
    #        client.V1EnvVar(
    #            name='NAMESPACE',
    #            value_from=client.V1EnvVarSource(
    #                field_ref=client.V1ObjectFieldSelector(
    #                    api_version='v1',
    #                    field_path='metadata.namespace')))],
    #    command=["ansible-playbook", "member-cert.yml"])
    #
    # statefulset.spec.template.spec.init_containers = [tls_init_container]

    return statefulset


def get_secret_object(cluster_object, name_suffix, string_data):
    name = cluster_object['metadata']['name']
    namespace = cluster_object['metadata']['namespace']

    secret = client.V1Secret()

    # Metadata
    secret.metadata = client.V1ObjectMeta(
        name='{}{}'.format(name, name_suffix),
        namespace=namespace,
        labels=get_default_labels(name=name))

    secret.string_data = string_data

    return secret
