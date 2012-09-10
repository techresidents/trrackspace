import re
import urlparse
   
def parse_url(url):
    (scheme, netloc, path, params, query, frag) = urlparse.urlparse(url)

    # We only support web services
    if not scheme in ('http', 'https'):
        raise ValueError('Scheme must be one of http or https')

    is_ssl = scheme == 'https' and True or False

    # Verify hostnames are valid and parse optional port
    match = re.match(r"([a-zA-Z0-9\-\.]+):?([0-9]{2,5})?", netloc)

    if match:
        (host, port) = match.groups()
        if not port:
            port = is_ssl and '443' or '80'
    else:
        raise ValueError('Invalid host and/or port: %s' % netloc)

    return (host, int(port), path.strip('/'), is_ssl)
