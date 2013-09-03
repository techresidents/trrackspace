from trpycore.factory.base import Factory

from trrackspace.services.identity.client import IdentityServiceClient

class IdentityServiceClientFactory(Factory):
    """Factory for creating IdentityServiceClient objects."""

    def __init__(self,
            username=None,
            api_key=None,
            password=None,
            endpoint=None,
            timeout=10,
            retries=2,
            keepalive=True,
            proxy=None,
            connection_class=None,
            debug_level=0):
        """IdentityServiceClientFactory constructor

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
            endpoint: optional api endpoint
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
        self.username = username
        self.api_key = api_key
        self.password = password
        self.endpoint = endpoint
        self.timeout = timeout
        self.retries = retries
        self.keepalive = keepalive
        self.proxy = proxy
        self.connection_class = connection_class
        self.debug_level = debug_level
        self.username = username

    def create(self):
        """Return instance of IdentityServiceClient object."""
        return IdentityServiceClient(
                username=self.username,
                api_key=self.api_key,
                password=self.password,
                endpoint=self.endpoint,
                timeout=self.timeout,
                retries=self.retries,
                keepalive=self.keepalive,
                proxy=self.proxy,
                connection_class=self.connection_class,
                debug_level=self.debug_level)
