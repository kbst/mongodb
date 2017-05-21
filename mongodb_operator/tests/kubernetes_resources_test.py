from unittest.mock import patch, call, MagicMock
from copy import deepcopy

from kubernetes import client

from ..memcached_operator.kubernetes_resources import (
get_default_labels, get_default_label_selector, get_service_object,
get_deployment_object)


class TestGetDefaultLabels():
    def test_returns_dict(self):
        assert isinstance(get_default_labels(), dict)

    def test_sets_operator(self):
        default_labels = get_default_labels()
        assert 'operated-by' in default_labels
        assert default_labels['operated-by'] == 'memcached.operator.kubestack.com'

    def test_sets_heritage(self):
        default_labels = get_default_labels()
        assert 'heritage' in default_labels
        assert default_labels['heritage'] == 'kubestack.com'

    def test_does_not_set_cluster(self):
        default_labels = get_default_labels()
        assert 'cluster' not in default_labels

    def test_with_name_sets_cluster(self):
        name = 'testname123'
        default_labels = get_default_labels(name=name)
        assert 'cluster' in default_labels
        assert default_labels['cluster'] == name


class TestGetDefaultLabelSelector():
    def test_returns_str(self):
        assert isinstance(get_default_label_selector(), str)

    def test_sets_operator(self):
        assert 'operated-by=memcached.operator.kubestack.com' in get_default_label_selector()

    def test_sets_heritage(self):
        assert 'heritage=kubestack.com' in get_default_label_selector()

    def test_does_not_set_cluster(self):
        assert 'cluster=' not in get_default_label_selector()

    def test_with_name_sets_cluster(self):
        name = 'testname123'
        assert 'cluster={}'.format(name) in get_default_label_selector(
            name=name)


class TestGetServiceObject():
    def setUp(self):
        self.name = 'testname123'
        self.namespace = 'testnamespace456'
        self.cluster_object = {'metadata': {'name': self.name,
                                            'namespace': self.namespace}}

    def test_returns_v1_service(self):
        service = get_service_object(self.cluster_object)
        assert isinstance(service, client.V1Service)

    def test_has_metadata(self):
        service = get_service_object(self.cluster_object)
        assert hasattr(service, 'metadata')
        assert isinstance(service.metadata, client.V1ObjectMeta)

    def test_has_metadata_name(self):
        service = get_service_object(self.cluster_object)
        assert service.metadata.name == self.name

    def test_has_metadata_namespace(self):
        service = get_service_object(self.cluster_object)
        assert service.metadata.namespace == self.namespace

    def test_has_metadata_labels(self):
        service = get_service_object(self.cluster_object)
        assert hasattr(service.metadata, 'labels')
        assert isinstance(service.metadata.labels, dict)
        default_labels = get_default_labels(name=self.name)
        for label in default_labels:
            assert label in service.metadata.labels
            assert service.metadata.labels[label] == default_labels[label]

    def test_has_monitoring_label(self):
        service = get_service_object(self.cluster_object)
        assert 'monitoring.kubestack.com' in service.metadata.labels
        assert service.metadata.labels['monitoring.kubestack.com'] == 'metrics'

    def test_has_spec(self):
        service = get_service_object(self.cluster_object)
        assert hasattr(service, 'spec')
        assert isinstance(service.spec, client.V1ServiceSpec)

    def test_has_spec_cluster_ip(self):
        service = get_service_object(self.cluster_object)
        assert hasattr(service.spec, 'cluster_ip')
        assert service.spec.cluster_ip == 'None'

    def test_has_spec_selector(self):
        service = get_service_object(self.cluster_object)
        assert hasattr(service.spec, 'selector')
        assert isinstance(service.spec.selector, dict)
        assert service.spec.selector == get_default_labels(name=self.name)

    def test_has_spec_ports(self):
        service = get_service_object(self.cluster_object)
        assert hasattr(service.spec, 'ports')
        assert isinstance(service.spec.ports, list)
        assert len(service.spec.ports) == 2

        assert isinstance(service.spec.ports[0], client.V1ServicePort)
        assert service.spec.ports[0].name == 'memcached'
        assert service.spec.ports[0].port == 11211
        assert service.spec.ports[0].protocol == 'TCP'

        assert isinstance(service.spec.ports[1], client.V1ServicePort)
        assert service.spec.ports[1].name == 'metrics'
        assert service.spec.ports[1].port == 9150
        assert service.spec.ports[1].protocol == 'TCP'

class TestGetDeploymentObject():
    def setUp(self):
        self.name = 'testname123'
        self.namespace = 'testnamespace456'
        self.cluster_object = {'metadata': {'name': self.name,
                                            'namespace': self.namespace}}

    def test_returns_v1beta1_deployment(self):
        deployment = get_deployment_object(self.cluster_object)
        assert isinstance(deployment, client.V1beta1Deployment)

    def test_has_metadata(self):
        deployment = get_deployment_object(self.cluster_object)
        assert hasattr(deployment, 'metadata')
        assert isinstance(deployment.metadata, client.V1ObjectMeta)

    def test_has_metadata_name(self):
        deployment = get_deployment_object(self.cluster_object)
        assert deployment.metadata.name == self.name

    def test_has_metadata_namespace(self):
        deployment = get_deployment_object(self.cluster_object)
        assert deployment.metadata.namespace == self.namespace

    def test_has_metadata_labels(self):
        deployment = get_deployment_object(self.cluster_object)
        assert hasattr(deployment.metadata, 'labels')
        assert isinstance(deployment.metadata.labels, dict)
        assert deployment.metadata.labels == get_default_labels(name=self.name)

    def test_has_spec(self):
        deployment = get_deployment_object(self.cluster_object)
        assert hasattr(deployment, 'spec')
        assert isinstance(deployment.spec, client.V1beta1DeploymentSpec)

    def test_has_spec_default_replicas(self):
        deployment = get_deployment_object(self.cluster_object)
        assert deployment.spec.replicas == 2

    def test_has_spec_custom_replicas(self):
        cluster_object = deepcopy(self.cluster_object)
        replicas = 8
        cluster_object['spec'] = {'memcached': {'replicas': replicas}}
        deployment = get_deployment_object(cluster_object)
        assert deployment.spec.replicas == replicas

    def test_has_spec_template(self):
        deployment = get_deployment_object(self.cluster_object)
        assert hasattr(deployment.spec, 'template')
        assert isinstance(deployment.spec.template, client.V1PodTemplateSpec)

    def test_has_spec_template_metadata(self):
        deployment = get_deployment_object(self.cluster_object)
        assert hasattr(deployment.spec.template, 'metadata')
        assert isinstance(
            deployment.spec.template.metadata, client.V1ObjectMeta)

    def test_has_spec_template_metadata_labels(self):
        deployment = get_deployment_object(self.cluster_object)
        metadata = deployment.spec.template.metadata
        assert hasattr(metadata, 'labels')
        assert isinstance(metadata.labels, dict)
        assert metadata.labels == get_default_labels(name=self.name)

    def test_has_pod_spec(self):
        deployment = get_deployment_object(self.cluster_object)
        template = deployment.spec.template
        assert hasattr(template, 'spec')
        assert isinstance(template.spec, client.V1PodSpec)

    def test_has_containers(self):
        deployment = get_deployment_object(self.cluster_object)
        spec = deployment.spec.template.spec
        assert hasattr(spec, 'containers')
        assert isinstance(spec.containers, list)
        assert len(spec.containers) == 2
        for c in spec.containers:
            assert isinstance(c, client.V1Container)

    def test_memcached_container(self):
        deployment = get_deployment_object(self.cluster_object)
        container = deployment.spec.template.spec.containers[0]
        assert hasattr(container, 'name')
        assert container.name == 'memcached'

        assert hasattr(container, 'command')
        assert container.command == ['memcached', '-p', '11211']

        assert hasattr(container, 'image')
        assert container.image == 'memcached:1.4.33'

        assert hasattr(container, 'ports')
        assert len(container.ports) == 1
        assert isinstance(container.ports[0], client.V1ContainerPort)
        assert container.ports[0].name == 'memcached'
        assert container.ports[0].container_port == 11211
        assert container.ports[0].protocol == 'TCP'

        assert hasattr(container, 'resources')
        assert isinstance(container.resources, client.V1ResourceRequirements)
        assert container.resources.limits == {'cpu': '100m', 'memory': '64Mi'}
        assert container.resources.requests == {'cpu': '100m', 'memory': '64Mi'}

    def test_memcached_container_custom_limit_cpu(self):
        cluster_object = deepcopy(self.cluster_object)
        limit = '200m'
        cluster_object['spec'] = {'memcached': {'memcached_limit_cpu': limit}}
        deployment = get_deployment_object(cluster_object)
        assert deployment.spec.template.spec.containers[0].resources.limits['cpu'] == limit

    def test_memcached_container_custom_limit_memory(self):
        cluster_object = deepcopy(self.cluster_object)
        limit = '128Mi'
        cluster_object['spec'] = {'memcached': {'memcached_limit_memory': limit}}
        deployment = get_deployment_object(cluster_object)
        assert deployment.spec.template.spec.containers[0].resources.limits['memory'] == limit

    def test_metrics_container(self):
        deployment = get_deployment_object(self.cluster_object)
        container = deployment.spec.template.spec.containers[1]
        assert hasattr(container, 'name')
        assert container.name == 'prometheus-exporter'

        assert hasattr(container, 'image')
        assert container.image == 'prom/memcached-exporter:v0.3.0'

        assert hasattr(container, 'ports')
        assert len(container.ports) == 1
        assert isinstance(container.ports[0], client.V1ContainerPort)
        assert container.ports[0].name == 'metrics'
        assert container.ports[0].container_port == 9150
        assert container.ports[0].protocol == 'TCP'

        assert hasattr(container, 'resources')
        assert isinstance(container.resources, client.V1ResourceRequirements)
        assert container.resources.limits == {'cpu': '50m', 'memory': '16Mi'}
        assert container.resources.requests == {'cpu': '50m', 'memory': '16Mi'}
