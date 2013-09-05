import json

from trhttp.rest.client import RestClient

from trrackspace.errors import to_error
from trrackspace.services.identity.client import IdentityServiceClient
from trrackspace.services.cloudfiles.container import Container

class CloudfilesClient(object):
    """Rackspace Cloudfiles API Client

    CloudfilesClient must be used in conjunction with an IdentityServiceClient.
    The identity_client is required for authentication and api endpoint
    lookups. If an identity_client is not explicitly passed to the construtor
    one will be created for you using the username and password/api_key.

    Example usage:
        client = CloudfilesClient(username="user", password="...")
        or
        client = CloudfilesClient(username="user", api_key="...")
        or
        identity_client = IdentityServiceClient(username="user", password="...")
        client = CloudfilesClient(identity_client=identity_client)
    """

    def __init__(self,
            username=None,
            api_key=None,
            password=None,
            region=None,
            servicenet=True,
            identity_client=None,
            identity_client_class=IdentityServiceClient,
            timeout=10,
            retries=2,
            keepalive=True,
            proxy=None,
            connection_class=None,
            debug_level=0):
        """CloudfilesClient constructor

        Args:
            username: Username to use for authentication with the Rackspace
                Identity Service. This argument is not required if 
                an identity_client argument is used.
            api_key: Api key to use for authentication with the Rackspace
                Identity Service. This argument is not required if a
                password or identity_client argument is used.
            password: Password to use for authentication with the Rackspace
                Identity Service. This argument is not required if an
                api_key or identity_client argument is used.
            region: optional datacenter region to connect to, i.e. DFW.
                If region is not passed, the default region for the
                authenticated user will be used.
            servicenet: boolean indicating the Rackspace internal network,
                servicenet, should be used for requests. If running
                on Rackspace servers this is recommended since latency
                will be lowered and charges will not be incurred.
            identity_client: optional IdentityServiceClient to use for
                authentication and api endpoint lookup. If identity_client
                is not passed, one will be created for you using the
                username and password/api_key pair.
            identity_client_class: optional IdentityServiceClient class
                to use to construct identity_client if it's not given.
            timeout: socket timeout in seconds
            retries: Number of times to try a request with an unexpected
                error before an exception is raised. Note that a value of 2
                means to try each api request twice (not 3 times) before
                raising an exception.
            keepalive: boolean indicating whether connections to the
                cloudfiles servers should be maintained between requests.
                If false, connections will be closed immediately following
                each api request.
            proxy: (host, port) tuple specifying proxy for connection
            connection_class: optional HTTP connectino class. It not 
                specified sensible default will be used.
            debug_level: httplib debug level. Setting this to 1 will log
                http requests and responses which is very useful for 
                debugging.
        """
        
        self.identity_client = identity_client
        self.object_count = 0
        self.bytes_used = 0
        self.container_count = 0
        self.metadata = {}
        
        if self.identity_client is None:
            self.identity_client = identity_client_class(
                    username=username,
                    api_key=api_key,
                    password=password,
                    timeout=timeout,
                    retries=retries,
                    keepalive=keepalive,
                    proxy=proxy,
                    connection_class=connection_class,
                    debug_level=debug_level)
        
        self.region = region or self.identity_client.user.default_region
        
        self.cloudfiles = Cloudfiles(
                region=self.region,
                servicenet=servicenet,
                identity_client=self.identity_client,
                timeout=timeout,
                retries=retries,
                keepalive=keepalive,
                proxy=proxy,
                connection_class=connection_class,
                debug_level=debug_level)

        self.cloudfiles_cdn = CloudfilesCdn(
                region=self.region,
                identity_client=self.identity_client,
                timeout=timeout,
                retries=retries,
                keepalive=keepalive,
                proxy=proxy,
                connection_class=connection_class,
                debug_level=debug_level)

        self.load()

    @to_error
    def load(self):
        """Load account data and metadata

        Issues a HEAD request to refresh account data such as 
        object-count, bytes-used, and metadata.

        Raises:
            ResponseError, RackspaceError
        """
        response_context = self.cloudfiles.send_request("HEAD", path="")
        with response_context as response:
            response.read()

            for name, value in response.getheaders():
                if name.lower() == "x-account-object-count":
                    self.object_count = int(value)
                elif name.lower() == "x-account-bytes-used":
                    self.bytes_used = int(value)
                elif name.lower() == "x-account-container-count":
                    self.container_count = int(value)
                elif name.lower().startswith("x-account-meta-"):
                    self.metadata[name.lower()] =  value;

    @to_error
    def get_temp_url_key(self):
        """Get account temp url key
        
        Account temp url key is required to generate temporary url's
        which are accessible regardless of CDN settings.

        Returns:
            account temp url key
        Raises:
            ResponseError, RackspaceError
        """
        return self.metadata.get("x-account-meta-temp-url-key")

    @to_error
    def set_temp_url_key(self, key):
        """Set account temp url key
        
        Account temp url key is required to generate temporary url's
        which are accessible regardless of CDN settings.

        Args:
            key: account temp url key
        Raises:
            ResponseError, RackspaceError
        """
        headers = {
            "x-account-meta-temp-url-key": key
        }
        response_context = self.cloudfiles.send_request(
                "POST", path="", headers=headers)
        with response_context as response:
            response.read()
        self.metadata.update(headers)

    @to_error
    def create_container(self, name):
        """Create Cloudfiles Container

        Args:
            name: Container name
        Returns:
            Container object
        Raises:
            ResponseError, RackspaceError
        """
        path = "/%s" % name
        response_context = self.cloudfiles.send_request("PUT", path)
        with response_context as response:
            response.read()
        return Container(self, name)

    @to_error
    def list_containers(self, limit=None, marker=None):
        """List Cloudfiles containers

        Args:
            limit: max results to returns
            marker: container name marking the container after which
                results whould be returned
        Returns:
            List of container info dicts, i.e.
            [ {u'bytes': 35515535291, u'count': 20, u'name': u'cloudservers'},
              {u'bytes': 8190746, u'count': 22, u'name': u'tr_private'},
              {u'bytes': 36319244, u'count': 66, u'name': u'tr_public'},
              {u'bytes': 291318, u'count': 8, u'name': u'trdev_private'},
              {u'bytes': 1305969, u'count': 24, u'name': u'trdev_public'},
              {u'bytes': 0, u'count': 0, u'name': u'trdev_static'}
            ]
        Raises:
            ResponseError, RackspaceError
        """
        params = { "format": "json" }
        if limit:
            params["limit"] = limit
        if marker:
            params["marker"] = marker

        response_context = self.cloudfiles.send_request(
                "GET", path="", params=params)
        with response_context as response:
            result = json.loads(response.read())
        return result

    @to_error
    def list_cdn_containers(self, limit=None, marker=None):
        """List Cloudfiles containers with CDN access enabled

        Args:
            limit: max results to returns
            marker: container name marking the container after which
                results whould be returned
        Returns:
            List of cdn container info dicts, i.e.
            [{ u'cdn_enabled': True,
               u'cdn_ios_uri': u'http://9399c0878ca7a44a7d7d.iosr.cf1.rackcdn.com',
               u'cdn_ssl_uri': u'https://c51dbe6cfe0485ea6e9c.ssl.cf1.rackcdn.com',
               u'cdn_streaming_uri': u'http://1765ae7e8b7c5fb0255ar88.stream.cf1.rackcdn.com',
               u'cdn_uri': u'http://1a173a41ad1e0a180c8e.r88.cf1.rackcdn.com',
               u'log_retention': False,
               u'name': u'Test Container',
               u'ttl': 86400}
            ]
        Raises:
            ResponseError, RackspaceError
        """
        params = { "format": "json" }
        if limit:
            params["limit"] = limit
        if marker:
            params["marker"] = marker

        response_context = self.cloudfiles_cdn.send_request("GET", "", params=params)
        with response_context as response:
            result = json.loads(response.read())
        return result
    
    @to_error
    def get_container(self, name, cdn_enabled=True):
        """Get Cloudfiles container

        Args:
            name: Container name
            cdn_enabled: optional flag indicated if the container is
                cdn_enabled. Setting this flag to True will result
                in an additional HEAD request to fetch the container's
                CDN metadata. Setting this flag to True on a container
                which is NOT cdn enabled, will NOT result in an exception,
                it's just slightly less efficient.
        Result:
            Container object
        Raises:
            NoSuchContainer, ResponseError, RackspaceError
        """
        return Container(client=self, name=name, cdn_enabled=cdn_enabled)

    @to_error
    def delete_container(self, name):
        """Delete empty Cloudfiles container.

        Args:
            name: Container name
        Raises:
            ContainerNotEmpty, ResponseError, RackspaceError
        """
        path = "/%s" % name
        response_context = self.cloudfiles.send_request("DELETE", path)
        with response_context as response:
            response.read()
                

class Cloudfiles(RestClient):
    """Cloudfiles Rest Client"""
    def __init__(self,
            region,
            identity_client,
            servicenet=True,
            endpoint=None,
            timeout=10,
            retries=1,
            keepalive=True,
            proxy=None,
            connection_class=None,
            debug_level=0):

        self.identity_client = identity_client
        
        if endpoint is None:
            service = self.identity_client.catalog.get_cloud_files()
            if service is None:
                self.identity_client.authenticate()
                service = self.identity_client.catalog.get_cloud_files()
            
            if servicenet:
                endpoint = service.endpoints.get_endpoint(region).internal_url
            else:
                endpoint = service.endpoints.get_endpoint(region).public_url
        
        super(Cloudfiles, self).__init__(
            endpoint=endpoint,
            timeout=timeout,
            retries=retries,
            keepalive=keepalive,
            proxy=proxy,
            connection_class=connection_class,
            authenticator=self.identity_client,
            debug_level=debug_level)


class CloudfilesCdn(RestClient):
    """Cloudfiles CDN Rest Client"""
    def __init__(self,
            region,
            identity_client,
            endpoint=None,
            timeout=10,
            retries=1,
            keepalive=True,
            proxy=None,
            connection_class=None,
            debug_level=0):

        self.identity_client = identity_client
        
        if endpoint is None:
            service = self.identity_client.catalog.get_cloud_files_cdn()
            if service is None:
                self.identity_client.authenticate()
                service = self.identity_client.catalog.get_cloud_files_cdn()
            
            endpoint = service.endpoints.get_endpoint(region).public_url

        super(CloudfilesCdn, self).__init__(
            endpoint=endpoint,
            timeout=timeout,
            retries=retries,
            keepalive=keepalive,
            proxy=proxy,
            connection_class=connection_class,
            authenticator=self.identity_client,
            debug_level=debug_level)
