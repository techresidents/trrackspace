import httplib
import json
import urllib2
import pprint
import re
import sys
import urlparse

USERNAME = 'techresidents'
API_KEY = '6e472c9131df23960b230bfd0b936ade'
AUTH_TOKEN = '6a892c10-c8a8-47e3-8ee6-04cc52de133f'
TRDEV_AUTH_TOKEN = '27c75bdd-acb3-452a-9a54-b07f3cb5b5fe'

def parse_url(url):
    """
    Given a URL, returns a 4-tuple containing the hostname, port,
    a path relative to root (if any), and a boolean representing
    whether the connection should use SSL or not.
    """
    (scheme, netloc, path, params, query, frag) = urlparse.urlparse(url)

    # We only support web services
    if not scheme in ('http', 'https'):
        raise ValueError('Scheme must be one of http or https')

    is_ssl = scheme == 'https' and True or False

    # Verify hostnames are valid and parse a port spec (if any)
    match = re.match('([a-zA-Z0-9\-\.]+):?([0-9]{2,5})?', netloc)

    if match:
        (host, port) = match.groups()
        if not port:
            port = is_ssl and '443' or '80'
    else:
        raise ValueError('Invalid host and/or port: %s' % netloc)

    return (host, int(port), path.strip('/'), is_ssl)

def authenticate(username=None, api_key=None):
    endpoint = "https://identity.api.rackspacecloud.com/v2.0/tokens"
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
    request = urllib2.Request(endpoint, data, headers=headers)
    result = urllib2.urlopen(request)
    response = json.loads(result.read())
    pprint.pprint(response)

def authenticate_password(username=None, password=None):
    endpoint = "https://identity.api.rackspacecloud.com/v2.0/tokens"
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
    request = urllib2.Request(endpoint, data, headers=headers)
    result = urllib2.urlopen(request)
    response = json.loads(result.read())
    pprint.pprint(response)

def list_users(auth_token):
    endpoint = "https://identity.api.rackspacecloud.com/v2.0/users"
    headers = {
        "X-Auth-Token": auth_token
    }
    #request = urllib2.Request(endpoint, headers=headers)
    #result = urllib2.urlopen(request)
    #response = json.loads(result.read())
    #pprint.pprint(response)
    
    host, port, path, is_ssl = parse_url(endpoint)
    connection = httplib.HTTPSConnection(host, port, timeout=5)
    connection.request('GET', '/v2.0/users', '', headers)
    result = connection.getresponse()
    response = json.loads(result.read())
    pprint.pprint(response)


def add_user(auth_token, username, password, email, enabled=True):
    endpoint = "https://identity.api.rackspacecloud.com/v2.0/users"
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
    pprint.pprint(data)

    headers = {
        "Content-type": "application/json",
        'Content-Length': str(len(data)),
        'X-Auth-Token': auth_token
   }

    host, port, path, is_ssl = parse_url(endpoint)
    connection = httplib.HTTPSConnection(host, port, timeout=5)
    connection.request('POST', '/v2.0/users', data, headers)
    result = connection.getresponse()
    response = json.loads(result.read())
    pprint.pprint(response)

def main(argv):
    #authenticate(username=USERNAME, api_key=API_KEY)
    #authenticate_password(username='trdev', password='s4jKI3lA&!')
    #list_users(AUTH_TOKEN)
    list_users(TRDEV_AUTH_TOKEN)
    #add_user(AUTH_TOKEN, 'trdev', 's4jKI3lA&!', 'techresidents@techresidents.com', True)
    #add_user(AUTH_TOKEN, 'trdev2', 's4jKI3lA&!', 'techresidents@techresidents.com', True)

if __name__ == "__main__":
    sys.exit(main(sys.argv))

