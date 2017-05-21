# flake8: noqa

from kubernetes.client.configuration import Configuration
from kubernetes.client.api_client import ApiClient

from six import iteritems


class MongoDBThirdPartyResourceV1Alpha1Api(object):

    def __init__(self, api_client=None):
        config = Configuration()
        if api_client:
            self.api_client = api_client
        else:
            if not config.api_client:
                config.api_client = ApiClient()
            self.api_client = config.api_client

    def list_mongodb_for_all_namespaces(self, **kwargs):
        """
        list or watch objects of kind Namespace
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please define a `callback` function
        to be invoked when receiving the response.
        >>> def callback_function(response):
        >>>     pprint(response)
        >>>
        >>> thread = api.list_namespace_with_http_info(callback=callback_function)

        :param callback function: The callback function
            for asynchronous request. (optional)
        :param str pretty: If 'true', then the output is pretty printed.
        :param str field_selector: A selector to restrict the list of returned objects by their fields. Defaults to everything.
        :param str label_selector: A selector to restrict the list of returned objects by their labels. Defaults to everything.
        :param str resource_version: When specified with a watch call, shows changes that occur after that particular version of a resource. Defaults to changes from the beginning of history.
        :param int timeout_seconds: Timeout for the list/watch call.
        :param bool watch: Watch for changes to the described resources and return them as a stream of add, update, and remove notifications. Specify resourceVersion.
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['pretty', 'field_selector', 'label_selector', 'resource_version', 'timeout_seconds', 'watch']
        all_params.append('callback')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method list_namespace" % key
                )
            params[key] = val
        del params['kwargs']


        collection_formats = {}

        resource_path = '/apis/operator.kubestack.com/v1/mongodbs/'.replace('{format}', 'json')
        path_params = {}

        query_params = {}
        if 'pretty' in params:
            query_params['pretty'] = params['pretty']
        if 'field_selector' in params:
            query_params['fieldSelector'] = params['field_selector']
        if 'label_selector' in params:
            query_params['labelSelector'] = params['label_selector']
        if 'resource_version' in params:
            query_params['resourceVersion'] = params['resource_version']
        if 'timeout_seconds' in params:
            query_params['timeoutSeconds'] = params['timeout_seconds']
        if 'watch' in params:
            query_params['watch'] = params['watch']

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.\
            select_header_accept(['application/json', 'application/yaml', 'application/vnd.kubernetes.protobuf', 'application/json;stream=watch', 'application/vnd.kubernetes.protobuf;stream=watch'])

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.\
            select_header_content_type(['*/*'])

        # Authentication setting
        auth_settings = ['BearerToken']

        (response) = self.api_client.call_api(resource_path, 'GET',
                                        path_params,
                                        query_params,
                                        header_params,
                                        body=body_params,
                                        post_params=form_params,
                                        files=local_var_files,
                                        response_type='object',
                                        auth_settings=auth_settings,
                                        callback=params.get('callback'),
                                        _return_http_data_only=True,
                                        _preload_content=params.get('_preload_content', True),
                                        _request_timeout=params.get('_request_timeout'),
                                        collection_formats=collection_formats)
        return response

    def read_namespaced_mongodb(self, name, namespace, **kwargs):
        """
        read the specified Pod
        This method makes a synchronous HTTP request by default. To make an
        asynchronous HTTP request, please define a `callback` function
        to be invoked when receiving the response.
        >>> def callback_function(response):
        >>>     pprint(response)
        >>>
        >>> thread = api.read_namespaced_pod_with_http_info(name, namespace, callback=callback_function)

        :param callback function: The callback function
            for asynchronous request. (optional)
        :param str name: name of the Pod (required)
        :param str namespace: object name and auth scope, such as for teams and projects (required)
        :param str pretty: If 'true', then the output is pretty printed.
        :param bool exact: Should the export be exact.  Exact export maintains cluster-specific fields like 'Namespace'
        :param bool export: Should this value be exported.  Export strips fields that a user can not specify.
        :return: V1Pod
                 If the method is called asynchronously,
                 returns the request thread.
        """

        all_params = ['name', 'namespace', 'pretty', 'exact', 'export']
        all_params.append('callback')
        all_params.append('_return_http_data_only')
        all_params.append('_preload_content')
        all_params.append('_request_timeout')

        params = locals()
        for key, val in iteritems(params['kwargs']):
            if key not in all_params:
                raise TypeError(
                    "Got an unexpected keyword argument '%s'"
                    " to method read_namespaced_pod" % key
                )
            params[key] = val
        del params['kwargs']
        # verify the required parameter 'name' is set
        if ('name' not in params) or (params['name'] is None):
            raise ValueError("Missing the required parameter `name` when calling `read_namespaced_pod`")
        # verify the required parameter 'namespace' is set
        if ('namespace' not in params) or (params['namespace'] is None):
            raise ValueError("Missing the required parameter `namespace` when calling `read_namespaced_pod`")


        collection_formats = {}

        resource_path = '/apis/operator.kubestack.com/v1/namespaces/{namespace}/mongodbs/{name}'.replace('{format}', 'json')
        path_params = {}
        if 'name' in params:
            path_params['name'] = params['name']
        if 'namespace' in params:
            path_params['namespace'] = params['namespace']

        query_params = {}
        if 'pretty' in params:
            query_params['pretty'] = params['pretty']
        if 'exact' in params:
            query_params['exact'] = params['exact']
        if 'export' in params:
            query_params['export'] = params['export']

        header_params = {}

        form_params = []
        local_var_files = {}

        body_params = None
        # HTTP header `Accept`
        header_params['Accept'] = self.api_client.\
            select_header_accept(['application/json', 'application/yaml', 'application/vnd.kubernetes.protobuf'])

        # HTTP header `Content-Type`
        header_params['Content-Type'] = self.api_client.\
            select_header_content_type(['*/*'])

        # Authentication setting
        auth_settings = ['BearerToken']

        (response) =  self.api_client.call_api(resource_path, 'GET',
                                        path_params,
                                        query_params,
                                        header_params,
                                        body=body_params,
                                        post_params=form_params,
                                        files=local_var_files,
                                        response_type='object',
                                        auth_settings=auth_settings,
                                        callback=params.get('callback'),
                                        _return_http_data_only=True,
                                        _preload_content=params.get('_preload_content', True),
                                        _request_timeout=params.get('_request_timeout'),
                                        collection_formats=collection_formats)
        return response
