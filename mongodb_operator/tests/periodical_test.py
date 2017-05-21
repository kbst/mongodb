from unittest.mock import patch, call, MagicMock
from copy import deepcopy

from kubernetes import client

from ..memcached_operator.periodical import (is_version_cached, cache_version,
                                             check_existing, collect_garbage)


class TestVersionCache():
    def setUp(self):
        self.uid = 'test-uid-1234567890'
        self.version = '123'
        self.resource = MagicMock()
        self.resource.metadata.uid = self.uid
        self.resource.metadata.version = self.version

    def test_version_not_cached(self):
        result = is_version_cached(self.resource)

        assert result is False

    def test_version_cached(self):
        cache_version(self.resource)
        result = is_version_cached(self.resource)

        assert result is True


class TestCheckExisting():
    def setUp(self):
        self.name = 'testname123'
        self.namespace = 'testnamespace456'
        self.cluster_object = {'metadata':{'name': self.name,
                                           'namespace': self.namespace}}
        self.base_list_result = {'items': [self.cluster_object]}

    @patch('memcached_operator.memcached_operator.periodical.logging')
    @patch('memcached_operator.memcached_operator.memcached_tpr_v1alpha1_api.MemcachedThirdPartyResourceV1Alpha1Api.list_memcached_for_all_namespaces', side_effect=client.rest.ApiException())
    def test_list_memcached_exception(self, mock_list_memcached_for_all_namespaces, mock_logging):
        result = check_existing()

        assert mock_logging.exception.called is True
        assert result is False

    @patch('memcached_operator.memcached_operator.periodical.update_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_deployment')
    @patch('kubernetes.client.ExtensionsV1beta1Api.read_namespaced_deployment')
    @patch('memcached_operator.memcached_operator.periodical.update_service')
    @patch('memcached_operator.memcached_operator.periodical.is_version_cached')
    @patch('memcached_operator.memcached_operator.periodical.cache_version')
    @patch('memcached_operator.memcached_operator.periodical.create_service')
    @patch('kubernetes.client.CoreV1Api.read_namespaced_service')
    @patch('memcached_operator.memcached_operator.memcached_tpr_v1alpha1_api.MemcachedThirdPartyResourceV1Alpha1Api.list_memcached_for_all_namespaces')
    def test_no_memcached_tprs(self, mock_list_memcached_for_all_namespaces, mock_read_namespaced_service, mock_create_service, mock_cache_version, mock_is_version_cached, mock_update_service, mock_read_namespaced_deployment, mock_create_deployment, mock_update_deployment):
        # Mock list memcached call with 0 items
        no_item_result = deepcopy(self.base_list_result)
        no_item_result['items'] = []
        mock_list_memcached_for_all_namespaces.return_value = no_item_result

        check_existing()

        mock_list_memcached_for_all_namespaces.assert_called_once_with()
        assert mock_read_namespaced_service.called is False
        assert mock_create_service.called is False
        assert mock_cache_version.called is False
        assert mock_is_version_cached.called is False
        assert mock_update_service.called is False
        assert mock_read_namespaced_deployment.called is False
        assert mock_create_deployment.called is False
        assert mock_update_deployment.called is False

    @patch('memcached_operator.memcached_operator.periodical.update_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_deployment', return_value=client.V1beta1Deployment())
    @patch('kubernetes.client.ExtensionsV1beta1Api.read_namespaced_deployment', side_effect=client.rest.ApiException(status=404))
    @patch('memcached_operator.memcached_operator.periodical.update_service')
    @patch('memcached_operator.memcached_operator.periodical.is_version_cached', return_value=True)
    @patch('memcached_operator.memcached_operator.periodical.cache_version')
    @patch('memcached_operator.memcached_operator.periodical.create_service', return_value=client.V1Service())
    @patch('kubernetes.client.CoreV1Api.read_namespaced_service', side_effect=client.rest.ApiException(status=404))
    @patch('memcached_operator.memcached_operator.memcached_tpr_v1alpha1_api.MemcachedThirdPartyResourceV1Alpha1Api.list_memcached_for_all_namespaces')
    def test_service_and_deploy_404(self, mock_list_memcached_for_all_namespaces, mock_read_namespaced_service, mock_create_service, mock_cache_version, mock_is_version_cached, mock_update_service, mock_read_namespaced_deployment, mock_create_deployment, mock_update_deployment):
        # Mock list memcached call with 0 items
        mock_list_memcached_for_all_namespaces.return_value = self.base_list_result

        check_existing()

        mock_list_memcached_for_all_namespaces.assert_called_once_with()
        mock_read_namespaced_service.assert_called_once_with(self.name, self.namespace)
        mock_create_service.assert_called_once_with(self.cluster_object)

        cache_version_calls = [
            call(client.V1Service()), call(client.V1beta1Deployment())]
        mock_cache_version.assert_has_calls(cache_version_calls)

        assert mock_is_version_cached.called is False
        assert mock_update_service.called is False
        mock_read_namespaced_deployment.assert_called_once_with(self.name, self.namespace)
        mock_create_deployment.assert_called_once_with(self.cluster_object)
        assert mock_update_deployment.called is False

    @patch('memcached_operator.memcached_operator.periodical.update_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_deployment', return_value=False)
    @patch('kubernetes.client.ExtensionsV1beta1Api.read_namespaced_deployment', side_effect=client.rest.ApiException(status=404))
    @patch('memcached_operator.memcached_operator.periodical.update_service')
    @patch('memcached_operator.memcached_operator.periodical.is_version_cached', return_value=True)
    @patch('memcached_operator.memcached_operator.periodical.cache_version')
    @patch('memcached_operator.memcached_operator.periodical.create_service', return_value=False)
    @patch('kubernetes.client.CoreV1Api.read_namespaced_service', side_effect=client.rest.ApiException(status=404))
    @patch('memcached_operator.memcached_operator.memcached_tpr_v1alpha1_api.MemcachedThirdPartyResourceV1Alpha1Api.list_memcached_for_all_namespaces')
    def test_service_and_deploy_404_yet_create_false(self, mock_list_memcached_for_all_namespaces, mock_read_namespaced_service, mock_create_service, mock_cache_version, mock_is_version_cached, mock_update_service, mock_read_namespaced_deployment, mock_create_deployment, mock_update_deployment):
        # Mock list memcached call with 0 items
        mock_list_memcached_for_all_namespaces.return_value = self.base_list_result

        check_existing()

        mock_list_memcached_for_all_namespaces.assert_called_once_with()
        mock_read_namespaced_service.assert_called_once_with(self.name, self.namespace)
        mock_create_service.assert_called_once_with(self.cluster_object)

        assert mock_cache_version.called is False

        assert mock_is_version_cached.called is False
        assert mock_update_service.called is False
        mock_read_namespaced_deployment.assert_called_once_with(self.name, self.namespace)
        mock_create_deployment.assert_called_once_with(self.cluster_object)
        assert mock_update_deployment.called is False

    @patch('memcached_operator.memcached_operator.periodical.logging')
    @patch('memcached_operator.memcached_operator.periodical.update_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_deployment')
    @patch('kubernetes.client.ExtensionsV1beta1Api.read_namespaced_deployment', side_effect=client.rest.ApiException(status=500))
    @patch('memcached_operator.memcached_operator.periodical.update_service')
    @patch('memcached_operator.memcached_operator.periodical.is_version_cached')
    @patch('memcached_operator.memcached_operator.periodical.cache_version')
    @patch('memcached_operator.memcached_operator.periodical.create_service')
    @patch('kubernetes.client.CoreV1Api.read_namespaced_service', side_effect=client.rest.ApiException(status=500))
    @patch('memcached_operator.memcached_operator.memcached_tpr_v1alpha1_api.MemcachedThirdPartyResourceV1Alpha1Api.list_memcached_for_all_namespaces')
    def test_service_and_deploy_500(self, mock_list_memcached_for_all_namespaces, mock_read_namespaced_service, mock_create_service, mock_cache_version, mock_is_version_cached, mock_update_service, mock_read_namespaced_deployment, mock_create_deployment, mock_update_deployment, mock_logging):
        # Mock list memcached call
        mock_list_memcached_for_all_namespaces.return_value = self.base_list_result

        check_existing()

        mock_list_memcached_for_all_namespaces.assert_called_once_with()
        mock_read_namespaced_service.assert_called_once_with(self.name, self.namespace)
        assert mock_create_service.called is False
        assert mock_cache_version.called is False
        assert mock_is_version_cached.called is False
        assert mock_update_service.called is False
        mock_read_namespaced_deployment.assert_called_once_with(self.name, self.namespace)
        assert mock_create_deployment.called is False
        assert mock_update_deployment.called is False
        mock_logging.exception.call_count == 2

    @patch('memcached_operator.memcached_operator.periodical.update_deployment')
    @patch('memcached_operator.memcached_operator.periodical.create_deployment')
    @patch('kubernetes.client.ExtensionsV1beta1Api.read_namespaced_deployment', return_value=client.V1beta1Deployment())
    @patch('memcached_operator.memcached_operator.periodical.update_service')
    @patch('memcached_operator.memcached_operator.periodical.is_version_cached', return_value=True)
    @patch('memcached_operator.memcached_operator.periodical.cache_version')
    @patch('memcached_operator.memcached_operator.periodical.create_service')
    @patch('kubernetes.client.CoreV1Api.read_namespaced_service', return_value=client.V1Service())
    @patch('memcached_operator.memcached_operator.memcached_tpr_v1alpha1_api.MemcachedThirdPartyResourceV1Alpha1Api.list_memcached_for_all_namespaces')
    def test_service_and_deploy_cached(self, mock_list_memcached_for_all_namespaces, mock_read_namespaced_service, mock_create_service, mock_cache_version, mock_is_version_cached, mock_update_service, mock_read_namespaced_deployment, mock_create_deployment, mock_update_deployment):
        # Mock list memcached call with 0 items
        mock_list_memcached_for_all_namespaces.return_value = self.base_list_result

        check_existing()

        mock_list_memcached_for_all_namespaces.assert_called_once_with()
        mock_read_namespaced_service.assert_called_once_with(self.name, self.namespace)
        assert mock_create_service.called is False
        assert mock_cache_version.called is False

        is_version_cached_calls = [
            call(client.V1Service()),
            call(client.V1beta1Deployment())]
        mock_is_version_cached.assert_has_calls(is_version_cached_calls)

        assert mock_update_service.called is False
        mock_read_namespaced_deployment.assert_called_once_with(self.name, self.namespace)
        assert mock_create_deployment.called is False
        assert mock_update_deployment.called is False

    @patch('memcached_operator.memcached_operator.periodical.update_deployment', return_value=client.V1beta1Deployment())
    @patch('memcached_operator.memcached_operator.periodical.create_deployment')
    @patch('kubernetes.client.ExtensionsV1beta1Api.read_namespaced_deployment', return_value=client.V1beta1Deployment())
    @patch('memcached_operator.memcached_operator.periodical.update_service', return_value=client.V1Service())
    @patch('memcached_operator.memcached_operator.periodical.is_version_cached', return_value=False)
    @patch('memcached_operator.memcached_operator.periodical.cache_version')
    @patch('memcached_operator.memcached_operator.periodical.create_service')
    @patch('kubernetes.client.CoreV1Api.read_namespaced_service', return_value=client.V1Service())
    @patch('memcached_operator.memcached_operator.memcached_tpr_v1alpha1_api.MemcachedThirdPartyResourceV1Alpha1Api.list_memcached_for_all_namespaces')
    def test_service_and_deploy_not_cached(self, mock_list_memcached_for_all_namespaces, mock_read_namespaced_service, mock_create_service, mock_cache_version, mock_is_version_cached, mock_update_service, mock_read_namespaced_deployment, mock_create_deployment, mock_update_deployment):
        # Mock list memcached call with 0 items
        mock_list_memcached_for_all_namespaces.return_value = self.base_list_result

        check_existing()

        mock_list_memcached_for_all_namespaces.assert_called_once_with()
        mock_read_namespaced_service.assert_called_once_with(self.name, self.namespace)
        assert mock_create_service.called is False

        cache_version_calls = [
            call(client.V1Service()), call(client.V1beta1Deployment())]
        mock_cache_version.assert_has_calls(cache_version_calls)

        is_version_cached_calls = [
            call(client.V1Service()),
            call(client.V1beta1Deployment())]
        mock_is_version_cached.assert_has_calls(is_version_cached_calls)

        mock_update_service.assert_called_once_with(self.cluster_object)
        mock_read_namespaced_deployment.assert_called_once_with(self.name, self.namespace)
        assert mock_create_deployment.called is False
        mock_update_deployment.assert_called_once_with(self.cluster_object)

    @patch('memcached_operator.memcached_operator.periodical.update_deployment', return_value=False)
    @patch('memcached_operator.memcached_operator.periodical.create_deployment')
    @patch('kubernetes.client.ExtensionsV1beta1Api.read_namespaced_deployment', return_value=client.V1beta1Deployment())
    @patch('memcached_operator.memcached_operator.periodical.update_service', return_value=False)
    @patch('memcached_operator.memcached_operator.periodical.is_version_cached', return_value=False)
    @patch('memcached_operator.memcached_operator.periodical.cache_version')
    @patch('memcached_operator.memcached_operator.periodical.create_service')
    @patch('kubernetes.client.CoreV1Api.read_namespaced_service', return_value=client.V1Service())
    @patch('memcached_operator.memcached_operator.memcached_tpr_v1alpha1_api.MemcachedThirdPartyResourceV1Alpha1Api.list_memcached_for_all_namespaces')
    def test_service_and_deploy_not_cached_yet_update_exception(self, mock_list_memcached_for_all_namespaces, mock_read_namespaced_service, mock_create_service, mock_cache_version, mock_is_version_cached, mock_update_service, mock_read_namespaced_deployment, mock_create_deployment, mock_update_deployment):
        # Mock list memcached call with 0 items
        mock_list_memcached_for_all_namespaces.return_value = self.base_list_result

        check_existing()

        mock_list_memcached_for_all_namespaces.assert_called_once_with()
        mock_read_namespaced_service.assert_called_once_with(self.name, self.namespace)
        assert mock_create_service.called is False

        print(mock_cache_version.call_args)
        assert mock_cache_version.called is False

        is_version_cached_calls = [
            call(client.V1Service()),
            call(client.V1beta1Deployment())]
        mock_is_version_cached.assert_has_calls(is_version_cached_calls)

        mock_update_service.assert_called_once_with(self.cluster_object)
        mock_read_namespaced_deployment.assert_called_once_with(self.name, self.namespace)
        assert mock_create_deployment.called is False
        mock_update_deployment.assert_called_once_with(self.cluster_object)


class TestCollectGargabe():
    def setUp(self):
        self.name = 'testname123'
        self.namespace = 'testnamespace456'

        svc_list = client.V1ServiceList()
        svc = client.V1Service()
        svc.metadata = client.V1ObjectMeta(
            name=self.name, namespace=self.namespace)
        svc_list.items = [svc]
        self.correct_svc_list = svc_list

        deploy_list = client.V1beta1DeploymentList()
        deploy = client.V1beta1Deployment()
        deploy.metadata = client.V1ObjectMeta(
            name=self.name, namespace=self.namespace)
        deploy_list.items = [deploy]
        self.correct_deploy_list = deploy_list

    @patch('memcached_operator.memcached_operator.periodical.reap_deployment')
    @patch('kubernetes.client.ExtensionsV1beta1Api.list_deployment_for_all_namespaces')
    @patch('memcached_operator.memcached_operator.memcached_tpr_v1alpha1_api.MemcachedThirdPartyResourceV1Alpha1Api.read_namespaced_memcached')
    @patch('memcached_operator.memcached_operator.periodical.delete_service')
    @patch('kubernetes.client.CoreV1Api.list_service_for_all_namespaces')
    def test_services_and_deployments_exceptions(self, mock_list_service_for_all_namespaces, mock_delete_service, mock_read_namespaced_memcached, mock_list_deployment_for_all_namespaces, mock_reap_deployment):
        # Mock service list exception
        mock_list_service_for_all_namespaces.side_effect = client.rest.ApiException()

        # Mock deployment list exception
        mock_list_deployment_for_all_namespaces.side_effect = client.rest.ApiException()

        collect_garbage()
        assert mock_read_namespaced_memcached.called is False
        assert mock_delete_service.called is False
        assert mock_reap_deployment.called is False

    @patch('memcached_operator.memcached_operator.periodical.reap_deployment')
    @patch('kubernetes.client.ExtensionsV1beta1Api.list_deployment_for_all_namespaces')
    @patch('memcached_operator.memcached_operator.memcached_tpr_v1alpha1_api.MemcachedThirdPartyResourceV1Alpha1Api.read_namespaced_memcached')
    @patch('memcached_operator.memcached_operator.periodical.delete_service')
    @patch('kubernetes.client.CoreV1Api.list_service_for_all_namespaces')
    def test_no_services_and_deployments(self, mock_list_service_for_all_namespaces, mock_delete_service, mock_read_namespaced_memcached, mock_list_deployment_for_all_namespaces, mock_reap_deployment):
        # Mock emtpy service list
        empty_svc_list = deepcopy(self.correct_svc_list)
        empty_svc_list.items = []
        mock_list_service_for_all_namespaces.return_value = empty_svc_list

        # Mock emtpy deployment list
        empty_deploy_list = deepcopy(self.correct_deploy_list)
        empty_deploy_list.items = []
        mock_list_deployment_for_all_namespaces.return_value = empty_deploy_list

        collect_garbage()
        assert mock_read_namespaced_memcached.called is False
        assert mock_delete_service.called is False
        assert mock_reap_deployment.called is False

    @patch('memcached_operator.memcached_operator.periodical.reap_deployment')
    @patch('kubernetes.client.ExtensionsV1beta1Api.list_deployment_for_all_namespaces')
    @patch('memcached_operator.memcached_operator.memcached_tpr_v1alpha1_api.MemcachedThirdPartyResourceV1Alpha1Api.read_namespaced_memcached')
    @patch('memcached_operator.memcached_operator.periodical.delete_service')
    @patch('kubernetes.client.CoreV1Api.list_service_for_all_namespaces')
    def test_expected_services_and_deployments(self, mock_list_service_for_all_namespaces, mock_delete_service, mock_read_namespaced_memcached, mock_list_deployment_for_all_namespaces, mock_reap_deployment):
        # Mock service list
        mock_list_service_for_all_namespaces.return_value = self.correct_svc_list

        # Mock deployment list
        mock_list_deployment_for_all_namespaces.return_value = self.correct_deploy_list

        collect_garbage()
        read_namespaced_memcached_calls = [
            call(self.name, self.namespace), call(self.name, self.namespace)]
        mock_read_namespaced_memcached.assert_has_calls(
            read_namespaced_memcached_calls)
        assert mock_delete_service.called is False
        assert mock_reap_deployment.called is False

    @patch('memcached_operator.memcached_operator.periodical.reap_deployment')
    @patch('kubernetes.client.ExtensionsV1beta1Api.list_deployment_for_all_namespaces')
    @patch('memcached_operator.memcached_operator.memcached_tpr_v1alpha1_api.MemcachedThirdPartyResourceV1Alpha1Api.read_namespaced_memcached')
    @patch('memcached_operator.memcached_operator.periodical.delete_service')
    @patch('kubernetes.client.CoreV1Api.list_service_for_all_namespaces')
    def test_unexpected_services_and_deployments(self, mock_list_service_for_all_namespaces, mock_delete_service, mock_read_namespaced_memcached, mock_list_deployment_for_all_namespaces, mock_reap_deployment):
        # Mock service list
        mock_list_service_for_all_namespaces.return_value = self.correct_svc_list

        # Mock deployment list
        mock_list_deployment_for_all_namespaces.return_value = self.correct_deploy_list

        # Mock read namespaced memcached side effect
        mock_read_namespaced_memcached.side_effect = client.rest.ApiException(status=404)

        collect_garbage()
        read_namespaced_memcached_calls = [
            call(self.name, self.namespace), call(self.name, self.namespace)]
        mock_read_namespaced_memcached.assert_has_calls(
            read_namespaced_memcached_calls)
        mock_delete_service.assert_called_once_with(self.name, self.namespace)
        mock_reap_deployment.assert_called_once_with(self.name, self.namespace)

    @patch('memcached_operator.memcached_operator.periodical.logging')
    @patch('memcached_operator.memcached_operator.periodical.reap_deployment')
    @patch('kubernetes.client.ExtensionsV1beta1Api.list_deployment_for_all_namespaces')
    @patch('memcached_operator.memcached_operator.memcached_tpr_v1alpha1_api.MemcachedThirdPartyResourceV1Alpha1Api.read_namespaced_memcached')
    @patch('memcached_operator.memcached_operator.periodical.delete_service')
    @patch('kubernetes.client.CoreV1Api.list_service_for_all_namespaces')
    def test_read_services_and_deployments_500(self, mock_list_service_for_all_namespaces, mock_delete_service, mock_read_namespaced_memcached, mock_list_deployment_for_all_namespaces, mock_reap_deployment, mock_logging):
        # Mock service list
        mock_list_service_for_all_namespaces.return_value = self.correct_svc_list

        # Mock deployment list
        mock_list_deployment_for_all_namespaces.return_value = self.correct_deploy_list

        # Mock read namespaced memcached side effect
        mock_read_namespaced_memcached.side_effect = client.rest.ApiException(status=500)

        collect_garbage()
        read_namespaced_memcached_calls = [
            call(self.name, self.namespace), call(self.name, self.namespace)]
        mock_read_namespaced_memcached.assert_has_calls(
            read_namespaced_memcached_calls)
        assert mock_delete_service.called is False
        assert mock_reap_deployment.called is False
        assert mock_logging.exception.called is True
        assert mock_logging.exception.call_count == 2
