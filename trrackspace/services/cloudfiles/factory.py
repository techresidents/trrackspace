from trpycore.factory.base import Factory

from trrackspace.services.cloudfiles.client import CloudfilesClient
from trrackspace.services.identity.client import IdentityServiceClient

class CloudfilesClientFactory(Factory):
    """Factory for creating CloudfileClient objects."""

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
        """CloudfilesClientFactory constructor

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
        self.username = username
        self.api_key = api_key
        self.password = password
        self.region = region
        self.servicenet = servicenet
        self.identity_client = identity_client
        self.identity_client_class = identity_client_class
        self.timeout = timeout
        self.retries = retries
        self.keepalive = keepalive
        self.proxy = proxy
        self.connection_class = connection_class
        self.debug_level = debug_level
        self.username = username

    def create(self):
        """Return instance of CloudfilesClient object."""
        return CloudfilesClient(
                username=self.username,
                api_key=self.api_key,
                password=self.password,
                region=self.region,
                servicenet=self.servicenet,
                identity_client=self.identity_client,
                identity_client_class=self.identity_client_class,
                timeout=self.timeout,
                retries=self.retries,
                keepalive=self.keepalive,
                proxy=self.proxy,
                connection_class=self.connection_class,
                debug_level=self.debug_level)
