import json

from trhttp.rest.client import RestClient
from trhttp.rest.auth import RestAuthenticator
from trhttp.errors import HttpError

from trrackspace.errors import to_error
from trrackspace.services.identity.catalog import ServiceCatalog
from trrackspace.services.identity.token import Token
from trrackspace.services.identity.user import User

class IdentityServiceClient(RestClient, RestAuthenticator):
    """Rackspace identity service client."""

    def __init__(self,
            username,
            api_key=None,
            password=None,
            endpoint=None,
            timeout=10,
            retries=2,
            keepalive=True,
            proxy=None,
            connection_class=None,
            debug_level=0):
        """IdentityServiceClient constructor

        Args:
            username: Username to use for authentication
            api_key: Api key to use for authentication.
                This argument is not required if a password is used.
            password: Password to use for authentication.
                This argument is not required if an api_key is used.
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

        #default objects which will be replaced following authentication
        self.catalog = ServiceCatalog()
        self.token = Token()
        self.user = User(self)

        if endpoint is None:
            endpoint = "https://identity.api.rackspacecloud.com/v2.0"

        super(IdentityServiceClient, self).__init__(
            endpoint=endpoint,
            timeout=timeout,
            retries=retries,
            keepalive=keepalive,
            proxy=proxy,
            connection_class=connection_class,
            authenticator=self,
            debug_level=debug_level)
    
    @to_error
    def authenticate(self, force=False):
        if not self.token.id or force:
            if self.api_key is not None:
                result = self.authenticate_api_key(
                        username=self.username,
                        api_key=self.api_key)
            elif self.password is not None:
                result = self.authenticate_password(
                        username=self.username,
                        password=self.password)
            else:
                raise ValueError()
            
            access = result.get("access")
            if access:
                self.catalog = ServiceCatalog.from_json(access.get("serviceCatalog"))
                self.user = User.from_json(self, access.get("user"))
                self.token = Token.from_json(access.get("token"))

        auth_headers = {
            "X-Auth-Token": self.token.id
        }
        
        return auth_headers

    @to_error
    def authenticate_api_key(self, username, api_key):
        data = json.dumps({
            "auth": {
                "RAX-KSKEY:apiKeyCredentials": {
                    "username": username,
                    "apiKey": api_key
                }
            }
        })

        headers = {
            "Content-type": "application/json"
        }
        

        response_context = self.send_request("POST", "/tokens", data, headers)
        with response_context as response:
            result = json.loads(response.read())
        return result

    @to_error
    def authenticate_password(self, username=None, password=None):
        data = json.dumps({
            "auth": {
                "passwordCredentials": {
                    "username": username,
                    "password": password
                }
            }
        })

        headers = {
            "Content-type": "application/json"
        }

        response_context = self.send_request("POST", "/tokens", data, headers)
        with response_context as response:
            result = json.loads(response.read())
        return result

    @to_error
    def get_user_by_name(self, username):
        path = "/users?name=%s" % username
        response_context = self.send_request("GET", path, None, None)
        with response_context as response:
            result = json.loads(response.read())
        return User.from_json(self, result["user"])


    @to_error
    def list_users(self):
        response_context = self.send_request("GET", "/users", None, None)
        with response_context as response:
            result = json.loads(response.read())

        users = []
        for user in result["users"]: 
            users.append(User.from_json(self, user))
        return users

    @to_error
    def add_user(self, username, email, password, enabled=True):
        data = json.dumps({
            "user": {
                "username": username,
                "OS-KSADM:password": password,
                "email": email,
                "enabled": enabled
            }
        })

        headers = {
            "Content-type": "application/json",
        }

        response_context = self.send_request("POST", "/users", data, headers)
        with response_context as response:
            result = json.loads(response.read())
        return User.from_json(self, result["user"])

    @to_error
    def delete_user(self, user_id):
        path = "/users/%s" % user_id
        response_context = self.send_request("DELETE", path, None, None)
        with response_context as response:
            result = response.read()
        return result
