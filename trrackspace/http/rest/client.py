from trrackspace.http.errors import HttpError
from trrackspace.http.utils import parse_url

class RestClient(object):
    def __init__(self,
            endpoint,
            authenticator=None,
            timeout=10,
            connection_class=None):
        self.endpoint = endpoint.rstrip("/")
        self.authenticator = authenticator
        self.connection_class = connection_class
        self.timeout = timeout
        self.auth_headers = None
        
        if self.connection_class is None:
            import httplib
            if endpoint.startswith("https:"):
                self.connection_class = httplib.HTTPSConnection
            else:
                self.connection_class = httplib.HTTPConnection
        
        #parse endpoint
        self.host, self.port, self.path, self.is_ssl = \
                parse_url(self.endpoint)
     
        #connection
        self.connection = self.connection_class(
                host=self.host,
                port=self.port,
                timeout=self.timeout)

        #authenticate
        self._authenticate()

    def _authenticate(self):
        if self.authenticator is not None:
            self.auth_headers = self.authenticator.authenticate()
    
    def default_headers(self, method, path, data):
        headers = {
            'Content-Length': str(len(data)),
        }

        if self.auth_headers:
            headers.update(self.auth_headers)
        return headers
    

    def send_request(self, method, path, data=None, headers=None):
        data = data or ""
        user_headers = headers
        headers = self.default_headers(method, path, data)
        if user_headers is not None:
            headers.update(user_headers)

        path = "/%s/%s" % (self.path, path.strip("/"))

        try:
            self.connection.request(method, path, data, headers)
            response = self.connection.getresponse()
            self.validate_response(response)
        except HttpError as error:
            if error.status == 401 and self.authenticator:
                self._authenticate()
                self.connection.request(method, path, data, headers)
                response = self.connection.getresponse()
                self.validate_response(response)
            else:
                raise
        return response

    def validate_response(self, response):
        if response.status < 200 or response.status > 299:
            data = response.read()
            raise HttpError(response.status, response.reason, data)
