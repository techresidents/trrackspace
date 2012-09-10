from trrackspace.http.rest.auth import RestAuthenticator
from trrackspace.identity import IdentityServiceClient

class RackspaceAuthenticator(RestAuthenticator):
    def __init__(self,
            username,
            api_key=None,
            password=None,
            auth_endpoint=None):

        self.username = username
        self.api_key = api_key
        self.password = password
        self.endpoint = auth_endpoint

    
    def authenticate(self):

        identity_service = IdentityServiceClient(
                endpoint=self.endpoint)

        try:

            if self.api_key is not None:
                response = identity_service.authenticate(
                        username=self.username,
                        api_key=self.api_key)
            elif self.password is not None:
                response = self.identity_service.authenticate_password(
                        username=self.username,
                        password=self.password)
            else:
                raise ValueError()
            
            auth_headers = {
                "X-Auth-Token": response["access"]["token"]["id"]
            }
        finally:
            identity_service.connection.close()
        
        return auth_headers
