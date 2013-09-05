import json
import urllib

from trhttp.errors import HttpError
from trrackspace.errors import to_error
from trrackspace.services.cloudfiles.errors import ContainerNotEmpty, \
        NoSuchContainer, NoSuchObject, ExtractArchiveError
from trrackspace.services.cloudfiles.storage_object import StorageObject

class Container(object):
    """Cloudfiles Container object

    This class should rarely, if ever, be constructed manually.
    Instead CloudfilesClient's create_container() or get_container()
    should be used.
    """

    def __init__(self, client, name, cdn_enabled=True):
        """Container constructor.

        Args:
            client: CloudfilesClient object
            name: Container name
            cdn_enabled: optional flag indicated if the container is
                cdn_enabled. Setting this flag to True will result
                in an additional HEAD request to fetch the container's
                CDN metadata. Setting this flag to True on a container
                which is NOT cdn enabled, will NOT result in an exception,
                it's just slightly less efficient.
        """
        self.client = client
        self.name = name
        self.cdn_enabled = cdn_enabled

        self.count = 0
        self.size = 0
        self.metadata = {}
        self._cdn_uri = None
        self._cdn_ssl_uri = None
        self._cdn_streaming_uri = None
        self.cdn_ttl = None
        self.cdn_log_retention = None

        self.load()
    
    @property
    def path(self):
        """Returns container API request path"""
        return self.name

    @property
    def uri(self):
        """Returns container non-cdn uri"""
        return "%s/%s" % (self.client.cloudfiles.endpoint, self.name)

    @property
    def cdn_uri(self):
        """Returns container cdn uri"""
        result = None
        if self.cdn_enabled:
            result = self._cdn_uri
        return result

    @property
    def cdn_ssl_uri(self):
        """Returns container cdn ssl uri"""
        result = None
        if self.cdn_enabled:
            result = self._cdn_ssl_uri
        return result

    @property
    def cdn_streaming_uri(self):
        """Returns container cdn streaming uri"""
        result = None
        if self.cdn_enabled:
            result = self._cdn_streaming_uri
        return result

    @to_error
    def load(self):
        """Load container data and metadata

        Issues HEAD requests to refresh container data such as 
        object-count, bytes-used, and metadata.

        Raises:
            ResponseError, RackspaceError
        """
        #refrech container data
        try:
            response_context = self.client.cloudfiles.send_request("HEAD", self.path)
            with response_context as response:
                response.read()

                self.metadata = {}
                for header in response.getheaders():
                    key = header[0].lower()
                    value = header[1]
                    if key == 'x-container-object-count':
                        self.count = int(value)
                    elif key == 'x-container-bytes-used':
                        self.size = int(header[1])
                    elif key.startswith('x-container-meta-'):
                        self.metadata[key.lower()] = value
        except HttpError as e:
            if e.status == 404:
                raise NoSuchContainer(self.name)
            else:
                raise

        if self.cdn_enabled:
            #refresh cdn container data
            try:
                response_context = self.client.cloudfiles_cdn.send_request("HEAD", self.path)
                with response_context as response:
                    response.read()

                    for header in response.getheaders():
                        key = header[0].lower()
                        value = header[1]
                        if key == "x-cdn-enabled":
                            self.cdn_enabled = True if value == "True" else False
                        elif key == "x-cdn-uri":
                            self._cdn_uri = value
                        elif key == "x-cdn-ssl-uri":
                            self._cdn_ssl_uri = value
                        elif key == "x-cdn-streaming-uri":
                            self._cdn_streaming_uri = value
                        elif key == "x-ttl":
                            self.cdn_ttl = int(value)
                        elif key == "x-log-retention":
                            self.cdn_log_retention = True if value == "True" else False
            except:
                #container is not cdn enabled
                self.cdn_enabled = False

    @to_error
    def list(self, prefix=None, limit=None,
            marker=None, end_marker=None, delimiter=None): 
        """List container storage objects by name
        
        Args:
            prefix: storage object name prefix which results must match
            limit: max number of results
            marker: storage object name marking the object after which
                results should be returned
            end_marker: storage object name marking the last object
                to return
            delimiter: path delimiter for filesystem like object listing.
                Setting delimiter='/' and prefix = 'static/' would list
                all storage objects in the logical 'static' directory.
        Returns:
            List of storage object names (max 10,000)
        Raises:
            ResponseError, RackspaceError
        Example Usage:
            >>> container.list()
            ['a.txt', 'tmp/b.txt', 'tmp/c.txt', 'tmp/sub/d.txt']
            >>> container.list(delimiter='/')
            ['a.txt', 'tmp/']
            >>> container.list(delimiter='/', prefix="tmp/")
            ['tmp/b.txt', 'tmp/c.txt', 'tmp/sub/']
        """
        params = {}

        if prefix:
            params["prefix"] = prefix
        if limit:
            params["limit"] = limit
        if marker:
            params["marker"] = marker
        if end_marker:
            params["end_marker"] = end_marker
        if delimiter:
            params["delimiter"] = delimiter

        response_context = self.client.cloudfiles.send_request(
                "GET", self.path, params=params)
        with response_context as response:
            result = response.read().splitlines()
        return result
    
    @to_error
    def list_objects(self, prefix=None, limit=None,
            marker=None, end_marker=None, delimiter=None): 
        """List container storage objects with info

        Args:
            prefix: storage object name prefix which results must match
            limit: max number of results
            marker: storage object name marking the object after which
                results should be returned
            end_marker: storage object name marking the last object
                to return
            delimiter: path delimiter for filesystem like object listing.
                Setting delimiter='/' and prefix = 'static/' would list
                all storage objects in the logical 'static' directory.
        Returns:
            List of storage object info dicts (max 10,000), i.e.
            [ {u'bytes': 4, u'last_modified': u'2013-08-27T20:14:50.378200',
               u'hash': u'8d777f385d3dfec8815d20f7496026dc', u'name': u'a.txt',
               u'content_type': u'text/plain'}
            ]    
        Raises:
            ResponseError, RackspaceError
        """
        params = { "format": "json" }

        if prefix:
            params["prefix"] = prefix
        if limit:
            params["limit"] = limit
        if marker:
            params["marker"] = marker
        if end_marker:
            params["end_marker"] = end_marker
        if delimiter:
            params["delimiter"] = delimiter

        response_context = self.client.cloudfiles.send_request(
                "GET", self.path, params=params)
        with response_context as response:
            result = json.loads(response.read())
        return result

    @to_error
    def list_all_objects(self, prefix=None, delimiter=None, batch_size=1000):
        """List all container storage objects with info
        
        This is a convenience method which will invoke list_objects() as many
        times as is necessary as determined by the batch_size to yield
        all of the object info dicts.

        Note that a genrator will be returned which will yield object info
        dicts. After every batch_size info dicts, an additional request
        will be made for the next batch of object info dicts.

        Args:
            prefix: storage object name prefix which results must match
            limit: max number of results
            delimiter: path delimiter for filesystem like object listing.
                Setting delimiter='/' and prefix = 'static/' would list
                all storage objects in the logical 'static' directory.
            batch_size: number of object info dicts to fetch with each
                api request. The number cannot exceed 10,000.
        Returns:
            Generator yielding storage object info dicts, i.e.
            [ {u'bytes': 4, u'last_modified': u'2013-08-27T20:14:50.378200',
               u'hash': u'8d777f385d3dfec8815d20f7496026dc', u'name': u'a.txt',
               u'content_type': u'text/plain'}
            ]    
        Raises:
            ResponseError, RackspaceError
        """
        marker=None
        while True:
            objects = self.list_objects(prefix=prefix,
                    limit=batch_size,
                    marker=marker,
                    delimiter=delimiter)
            
            for object in objects:
                yield object
            
            if len(objects) < batch_size:
                break

            marker = objects[-1]["name"]

    @to_error
    def create_object(self, name, content_type=None,
            metadata=None, cors=None, delete_at_timestamp=None):
        """Create a Cloudfiles storage object.
        
        Note that the storage object will not actually be created
        on Rackspace until the StorageObject.write() method is
        invoked.

        Args:
            name: storage object name
            content_type: content type
            metadata: dict of metadata headers
            cors: dict of CORS headers
            delete_at_timestamp: delete at timestamp
        Returns:
            StorageObject
        Raises:
            ResponseError, RackspaceError
        """
        return StorageObject(self, name, content_type=content_type,
                metadata=metadata, cors=cors,
                delete_at_timestamp=delete_at_timestamp, exists=False)
    
    @to_error
    def extract_archive(self, archive_path, type=None):
        """Extract a .tar, tar.gz, tar.bz archive to the container

        Args:
            archive_path: filesystem path to archive to extract
            type: type of archive, '.tar', '.tar.gz', '.tar.bz'.
                If not given it will be determined from the archive_path
        Returns:
            Extract archive result dict
        Raises:
            ExtractArchiveError, ResponseError, RackspaceError
        """
        if type is None:
            path = archive_path.lower()
            for suffix in [".tar", ".tar.gz", ".tar.bz"]:
                if path.endswith(suffix):
                    type = suffix
                    break
            else:
                raise ValueError("unsupported archive")
        
        
        headers = {"Accept": "application/json"}
        params = {"extract-archive": type}

        with open(archive_path, "r") as data:
            response_context = self.client.cloudfiles.send_request(
                    "PUT", self.path, data=data, headers=headers, params=params)
            with response_context as response:
                result = json.loads(response.read())
        
        if result.get("Errors"):
            raise ExtractArchiveError(result)

        return result
    
    @to_error
    def get_object(self, name):
        """Get storage object

        Args:
            name: Storage object name
        Returns:
            StorageObject
        Raises:
            NoSuchObject, ResponseError, RackspaceError
        """
        return StorageObject(self, name, exists=True)

    @to_error
    def delete_object(self, name):
        """Delete storage object

        Args:
            name: Storage object name
        Raises:
            NoSuchObject, ResponseError, RackspaceError
        """
        path = "%s/%s" % (self.path, urllib.quote(name))

        try:
            response_context = self.client.cloudfiles.send_request("DELETE", path)
            with response_context as response:
                response.read()
        except HttpError as error:
            if error.status == 404:
                raise NoSuchObject(name)
    
    @to_error
    def delete_objects(self, names):
        """Delete multiple storage objects using bulk delete

        Args:
            names: list of storage object names
        Raises:
            ResponseError, RackspaceError
        """
        object_paths = []
        for name in names:
            object_path = "%s/%s" % (self.path, urllib.quote(name))
            object_paths.append(object_path)
        data = "\n".join(object_paths)
        params = { "bulk-delete": "True" }

        response_context = self.client.cloudfiles.send_request(
                "DELETE", self.path, data=data, params=params)
        with response_context as response:
            response.read()
    
    @to_error
    def delete_all_objects(self, batch_size=1000):
        """Delete all storage objects in the container

        This is a convenience method which will delete list_objects() as many
        times as is necessary as determined by the batch_size to delete
        all of the objects within the container.

        Args:
            batch_size: maximum number of objects to delete
                in each api request
        Raises:
            ResponseError, RackspaceError
        """
        while True:
            object_names = self.list(limit=batch_size)
            if len(object_names):
                self.delete_objects(object_names)
            if len(object_names) < batch_size:
                break

    @to_error
    def delete(self):
        """Delete empty container.
        
        Note that the container must be empty to be deleted, otherwise
        a ContainerNotEmpty exception will be raised.
        Raises:
            ContainerNotEmpty, ResponseError, RackspaceError
        """
        try:
            response_context = self.client.cloudfiles.send_request("DELETE", self.path)
            with response_context as response:
                response.read()
        except HttpError as error:
            if error.status == 404:
                raise NoSuchContainer(self.name)
            elif error.status == 409:
                raise ContainerNotEmpty

    @to_error
    def update_metadata(self, metadata):
        """Update container metadata
        
        Container metadata keys must begin with 'x-container-meta-' or
        'x-remove-container-meta-' if you're removing metadata.

        Args:
            metadata: dict of metadata to update.
        Raises:
            ResponseError, RackspaceError
        """
        valid_prefixes = ["x-container-meta-", "x-remove-container-meta-"]
        for key in metadata or {}:
            key = key.lower()
            for prefix in valid_prefixes:
                if key.startswith(prefix):
                    break
            else:
                msg = "'%s' is invalid: must start with one of: %s" % \
                        (key, valid_prefixes)
                raise ValueError(msg)

        response_context = self.client.cloudfiles.send_request(
                "POST", self.path, headers=metadata)

        with response_context as response:
            response.read()
        
        #update metadata
        for key, value in metadata.items():
            if key.lower().startswith("x-remove-"):
                key = key[:2] + key[9:]
                self.metadata.pop(key, None)
            else:
                self.metadata[key] = value

    @to_error
    def enable_object_versioning(self, backup_container):
        """Enable Cloudfiles object versioning.
        
        Enabling object versioning on a container will cause the previous
        version of an object to be stored in backup_container when
        an object is CHANGED (not created).

        Deleting an object will restore its previous version from the
        backup_container. So in order to delete a versioned object
        you may have to call StorageObject.delete() multiple times.

        Args:
            backup_container: backup Container object
        Raises:
            ResponseError, RackspaceError
        """
        headers = { "x-versions-location": backup_container.name }
        response_context = self.client.cloudfiles.send_request(
                "POST", self.path, headers=headers)
        with response_context as response:
            response.read()

    @to_error
    def disable_object_versioning(self):
        """Disable Cloudfiles object versioning

        Raises:
            ResponseError, RackspaceError
        """
        headers = { "x-versions-location": "" }
        response_context = self.client.cloudfiles.send_request(
                "POST", self.path, headers=headers)
        with response_context as response:
            response.read()
    
    @to_error
    def enable_log_retention(self):
        """Enable Cloudfiles CDN log retention

        Raises:
            ResponseError, RackspaceError
        """
        headers = { "x-log-retention": "True" }
        response_context = self.client.cloudfiles_cdn.send_request(
                "POST", self.path, headers=headers)
        with response_context as response:
            response.read()
        self.cdn_log_retention = True

    @to_error
    def disable_log_retention(self):
        """Disable Cloudfiles CDN log retention

        Raises:
            ResponseError, RackspaceError
        """
        headers = { "x-log-retention": "False" }
        response_context = self.client.cloudfiles_cdn.send_request(
                "POST", self.path, headers=headers)
        with response_context as response:
            response.read()
        self.cdn_log_retention = False

    @to_error
    def enable_quota(self, max_bytes=None, max_object_count=None):
        """Enable Cloudfiles quota

        args:
            max_bytes: max number of bytes allowed
            max_object_count: max number of objects allowed    
        Raises:
            ResponseError, RackspaceError
        """
        headers = {}
        if max_bytes:
            headers["x-container-meta-quota-bytes"] = str(max_bytes)
        if max_object_count:
            headers["x-container-meta-quota-count"] = str(max_object_count)
        
        if headers:
            response_context = self.client.cloudfiles.send_request(
                    "PUT", self.path, headers=headers)
            with response_context as response:
                response.read()

        self.metadata.update(headers)
    
    @to_error
    def disable_quota(self):
        """Disable Cloudfiles quota

        Raises:
            ResponseError, RackspaceError
        """
        headers = {
            "x-remove-container-meta-quota-bytes": "True",
            "x-remove-container-meta-quota-count": "True"
        }
        response_context = self.client.cloudfiles.send_request(
                "PUT", self.path, headers=headers)
        with response_context as response:
            response.read()


    @to_error
    def enable_cdn(self, ttl=259200):
        """Enable Cloudfiles CDN access
        
        Args:
            ttl: CDN time to live in seconds.
                Changing this value will not remove content from the CDN.
        Raises:
            ResponseError, RackspaceError
        """
        headers = {
            "x-ttl": str(ttl),
            "x-cdn-enabled": 'True'
        }
        
        response_context = self.client.cloudfiles_cdn.send_request(
                "PUT", self.path, headers=headers)
        with response_context as response:
            response.read()
        
            self.cdn_enabled = True
            self.cdn_ttl = ttl

            for header in response.getheaders():
                key = header[0].lower()
                value = header[1]
                if key == "x-cdn-uri":
                    self._cdn_uri = value
                elif key == "x-cdn-ssl-uri":
                    self._cdn_ssl_uri = value
                elif key == "x-cdn-streaming-uri":
                    self._cdn_streaming_uri = value
                elif key == "x-ttl":
                    self.cdn_ttl = int(value)

        self.cdn_enabled = True

    @to_error
    def disable_cdn(self):
        """Disabled Cloudfiles CDN access.

        Note that this will not remove data currently on the CDN and such
        data will still be accessible until it is purged.
        
        Raises:
            ResponseError, RackspaceError
        """
        headers = {
            "x-cdn-enabled": "False"
        }
        
        response_context = self.client.cloudfiles_cdn.send_request(
                "PUT", self.path, headers=headers)
        with response_context as response:
            response.read()

        self.cdn_enabled = False

    @to_error
    def purge_from_cdn(self, email=None):
        """Purge all objects from the CDN.

        Note that this is an extremely expensive operation and should only
        be used if absolutely necessary.

        Args:
            Email: optional email address to notify when purge is completed
        Raises:
            ResponseError, RackspaceError
        """
        headers = {"x-purge-email": email} if email else None
        response_context = self.client.cloudfiles_cdn.send_request(
                "DELETE", self.path, headers=headers)
        with response_context as response:
            response.read()

    @to_error
    def enable_cors(self, allow_origin, max_age=None, allow_headers=None):
        """Enable Cross Orign Resource Sharing on CDN

        Raises:
            ResponseError, RackspaceError
        """
        if not isinstance(allow_origin, basestring):
            allow_origin = " ".join(allow_origin)
        headers = {
            "x-container-meta-access-control-allow-origin": allow_origin
        }
        if max_age:
            headers["x-container-meta-access-control-max-age"] = str(max_age)
        if allow_headers:
            if not isinstance(allow_headers, basestring):
                allow_origin = ",".join(allow_headers)
            headers["x-container-meta-access-control-max-age"] = allow_headers

        response_context = self.client.cloudfiles.send_request(
                "POST", self.path, headers=headers)
        with response_context as response:
            response.read()

    @to_error
    def disable_cors(self):
        """Disable Cross Orign Resource Sharing on CDN

        Raises:
            ResponseError, RackspaceError
        """
        headers = {
            "x-remove-container-meta-access-control-allow-origin": "True",
            "x-remove-container-meta-access-control-max-age": "True",
            "x-remove-container-meta-access-control-allow-headers": "True"
        }
        response_context = self.client.cloudfiles.send_request(
                "POST", self.path, headers=headers)
        with response_context as response:
            response.read()

    @to_error
    def enable_static_web(self, index, error=None):
        """Enable static web on CDN
        
        Args:
            index: index.html storage object name
            error: error storage object basename.
        Raises:
            ResponseError, RackspaceError
        """
        headers = {
            "x-container-meta-web-index": index
        }
        if error:
            headers["x-container-meta-web-error"] = error

        response_context = self.client.cloudfiles.send_request(
                "POST", self.path, headers=headers)
        with response_context as response:
            response.read()

    @to_error
    def disable_static_web(self):
        """Disable static web on CDN

        Raises:
            ResponseError, RackspaceError
        """
        headers = {
            "x-remove-container-meta-web-index": "True",
            "x-remove-container-meta-web-error": "True"
        }
        response_context = self.client.cloudfiles.send_request(
                "POST", self.path, headers=headers)
        with response_context as response:
            response.read()
