import json

from trrackspace.http.rest.client import RestClient
from trrackspace.http.rest.auth import RestAuthenticator

class IdentityServiceClient(RestClient, RestAuthenticator):
    def __init__(self,
            username,
            api_key=None,
            password=None,
            endpoint=None,
            connection_class=None,
            timeout=10):
        
        self.username = username
        self.api_key = api_key
        self.password = password

        if endpoint is None:
            endpoint = "https://identity.api.rackspacecloud.com/v2.0"

        super(IdentityServiceClient, self).__init__(
            endpoint=endpoint,
            connection_class=connection_class,
            timeout=timeout,
            authenticator=self)
    
    def authenticate(self):
        if self.api_key is not None:
            response = self.authenticate_api_key(
                    username=self.username,
                    api_key=self.api_key)
        elif self.password is not None:
            response = self.authenticate_password(
                    username=self.username,
                    password=self.password)
        else:
            raise ValueError()
        
        auth_headers = {
            "X-Auth-Token": response["access"]["token"]["id"]
        }
        
        return auth_headers

    def authenticate_api_key(self, username, api_key):
        data = """
        {
            "auth": {
            "RAX-KSKEY:apiKeyCredentials": {
                "username": "%s",
                "apiKey": "%s"
                }
            }
        }
        """ % (username, api_key)

        headers = {
            "Content-type": "application/json"
        }
        
        response = self.send_request("POST", "/tokens", data, headers)
        result = json.loads(response.read())
        return result

    def authenticate_password(self, username=None, password=None):
        data = """
        {
            "auth": {
            "passwordCredentials": {
                "username": "%s",
                "password": "%s"
                }
            }
        }
        """ % (username, password)
        headers = {
            "Content-type": "application/json"
        }

        response = self.send_request("POST", "/tokens", data, headers)
        result = json.loads(response.read())
        return result

    def list_users(self):
        response = self.send_request("GET", "/users", None, None)
        result = json.loads(response.read())
        return result

    def list_credentials(self, user_id):
        path = "/users/%s/OS-KSADM/credentials" % user_id
        response = self.send_request("GET", path, None, None)
        result = json.loads(response.read())
        return result


    def add_user(self, username, password, email, enabled=True):
        data = """
        {
            "user": {
            "username": "%s",
            "email": "%s",
            "enabled": %s,
            "OS-KSADM:password":"%s"
            }
        }
        """ % (username, email, 'true' if enabled else 'false', password)
    
        headers = {
            "Content-type": "application/json",
        }

        response = self.send_request("POST", "/users", data, headers)
        result = json.loads(response.read())
        return result

    def update_user(self, user_id, username, email, enabled=True, default_region="DFW"):
        path = "/users/%s" % user_id

        data = """
        {
            "user": {
            "username": "%s",
            "email": "%s",
            "enabled": %s,
            "RAX-AUTH:defaultRegion": "%s"
            }
        }
        """ % (username, email, 'true' if enabled else 'false', default_region)

        headers = {
            "Content-type": "application/json"
        }

        response = self.send_request("POST", path, data, headers)
        result = json.loads(response.read())
        return result

    def delete_user(self, user_id):
        path = "/users/%s" % user_id
        response = self.send_request("DELETE", path, None, None)
        result = response.read()
        return result
