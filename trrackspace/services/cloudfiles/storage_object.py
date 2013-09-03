import hashlib
import hmac
import mimetypes
import time
import urllib

from trhttp.errors import HttpError
from trpycore.chunk.basic import BasicChunker
from trpycore.chunk.hash import HashChunker

from trrackspace.errors import to_error
from trrackspace.services.cloudfiles.errors import NoSuchObject

class StorageObject(object):
    """Cloudfiles storage object"""
    def __init__(self, container, name, exists=False,
            content_type=None, metadata=None, delete_at_timestamp=None):
        """StorageObject constructor

            Args:
                container: Container object
                name: storage object name
                exists: boolean indicating if the object exists in
                    Cloudfiles and its metadata should be loaded.
                content_type: optional content type. If not provided
                    it will be determined by its name.
                metadata: dict of object metadata headers
                delete_at_timestamp: unix timestamp at which object
                    should be deleted.
            Raises:
                NoSuchObject if exists=True and object does not exist
                ResponseError, RackspaceError
        """
        self.container = container
        self.name = name
        self.exists = exists
        self.content_type = content_type or \
                mimetypes.guess_type(name)[0] or \
                "application/octet-stream"
        self.metadata = metadata or {}
        self.delete_at_timestamp = delete_at_timestamp

        self.manifest = None
        self.content_length = 0
        self.last_modified = None
        self.etag = None
        
        if self.exists:
            self.load()
    
    @property 
    def path(self):
        """Returns storage object path"""
        return "%s/%s" % \
                (self.container.path, urllib.quote(self.name))

    @property
    def size(self):
        return self.content_length

    @property
    def uri(self):
        """Returns storage object uri"""
        return "%s/%s" % \
                (self.container.uri, urllib.quote(self.name))

    @property
    def cdn_uri(self):
        """Returns storage object cdn uri"""
        result = None
        if self.container.cdn_uri:
            result = "%s/%s" % \
                    (self.container.cdn_uri, urllib.quote(self.name))
        return result

    @property
    def cdn_ssl_uri(self):
        """Returns storage object cdn ssl uri"""
        result = None
        if self.container.cdn_ssl_uri:
            result = "%s/%s" % \
                    (self.container.cdn_uri, urllib.quote(self.name))
        return result

    @property
    def cdn_streaming_uri(self):
        """Returns storage object cdn streaming uri"""
        result = None
        if self.container.cdn_streaming_uri:
            result = "%s/%s" % \
                    (self.container.cdn_streaming_uri, urllib.quote(self.name))
        return result

    @to_error
    def load(self):
        """Load storage object data and metadata

        Issue a HEAD request to load object data and metadata.

        Raises:
            NoSuchObject, ResponseError, RackspaceError
        """
        try:
            cloudfiles = self.container.client.cloudfiles
            response_context = cloudfiles.send_request("HEAD", self.path)
            with response_context as response:
                response.read()

                self.metadata = {}
                for header in response.getheaders():
                    key = header[0].lower()
                    value = header[1]
                    if key == 'x-object-manifeset':
                        self.manifest = value
                    elif key == 'content-type':
                        self.content_type = value
                    elif key == 'content-length':
                        self.content_length = int(value)
                    elif key == 'last-modified':
                        self.last_modified = value
                    elif key == 'etag':
                        self.etag = value
                    elif key == 'x-delete-at':
                        self.delete_at_timestamp = int(value)
                    elif key.startswith('x-object-meta-'):
                        self.metadata[key.lower()] = value
        except HttpError as e:
            if e.status == 404:
                raise NoSuchObject(self.name)
            else:
                raise

    @to_error
    def temp_url(self, method, seconds, filename=None):
        """Return temp url where object can be accessed
        
        Generates a temporary url where the object can be accessed
        regardless of whether CDN is enabled. Generating a temp
        url does not require any I/O, but it does require that
        the client temporary url key is set to properly sign
        the url's.
        
        Args:
            method: "GET" or "PUT" to generate URL for
            seconds: number of seconds url is valid for
            filename: optional filename override to use in browser
        Returns:
            temporary url to object
        Raises:
            ResponseError, RackspaceError
        """
        path = self.uri.split(".com")[1]
        key = self.container.client.get_temp_url_key()
        expires = int(time.time() + seconds)
        hmac_data = "%s\n%s\n%s" % (method, expires, path)
        signature = hmac.new(key, hmac_data, hashlib.sha1).hexdigest()

        urlparams = {
            "temp_url_sig": signature,
            "temp_url_expires": expires
        }

        if filename:
            urlparams["filename"] = filename

        return "%s?%s" % (self.uri, urllib.urlencode(urlparams))

    @to_error
    def read(self, size=None, offset=0, output=None, output_chunk_size=65535):
        """Read storage object data

            Args:
                size: number of bytes to read
                offset: offset in bytes to read from
                output: optional file-like output object to write read
                    data to. If not given, read data will be returned.
                output_chunk_size: chunk size to use when writing data
                    to output.
            Returns:
                Read data or output object if given.
            Raises:
                ResponseError, RackspaceError
        """
        cloudfiles = self.container.client.cloudfiles
        headers = {}
        if size:
            headers["Range"] = "bytes=%d-%d" % (offset, offset+size-1)
        elif offset > 0:
            headers["Range"] = "bytes=%d-" % (offset)
        elif offset < 0:
            headers["Range"] = "bytes=%d" % (offset)

        response_context = cloudfiles.send_request("GET", self.path, None, headers)
        with response_context as response:
            if output:
                chunker = BasicChunker(response)
                for chunk in chunker.chunks(output_chunk_size):
                    output.write(chunk)
                result = output
            else:
                result = response.read()

        return result

    @to_error
    def chunks(self, chunk_size=65535, size=None, offset=0):
        """Return generator yielding chunk_size buffers of read data.
            
            Note that a single HTTP "GET" request will be used for this operation 
            with chunk_size response reads. This means that the API request
            will NOT be terminated until all generated chunks are consumed,
            and additional requests using the same client will not be 
            possible.

            Args:
                chunk_size: chunk size in bytes of data to yield
                size: total number of bytes to read
                offset: offset in bytes to read from
            Returns:
                Generator yielding chunk_size buffers of data
        """
        cloudfiles = self.container.client.cloudfiles
        headers = {}
        if size:
            headers["Range"] = "bytes=%d-%d" % (offset, offset+size-1)
        elif offset > 0:
            headers["Range"] = "bytes=%d-" % (offset)
        elif offset < 0:
            headers["Range"] = "bytes=%d" % (offset)

        response_context = cloudfiles.send_request("GET", self.path, None, headers)
        with response_context as response:
            chunker = BasicChunker(response)
            for chunk in chunker.chunks(chunk_size):
                yield chunk

    @to_error
    def write(self, data, data_size=None, verify=True, chunk_size=65535):
        """Write data to storage object.

        Args:
            data: string, Chunker, or file-like object of data to write.
            data_size: optional data_size to use in Content-Length 
                header. If not set (and not a string), HTTP chunked
                encoding will be used.
            verify: boolean indicating if etag containing checksum
                should be validated
            chunk_size: chunk size to use in HTTP data xfer
        Raises:
            ResponseError, RackspaceError
        """
        cloudfiles = self.container.client.cloudfiles
        headers = {
            "Content-Type": self.content_type
        }
        headers.update(self.metadata)
        if self.delete_at_timestamp:
            header["x-delete-at"] = str(int(self.delete_at_timestamp))
        
        if verify:
            data = HashChunker(data)
        else:
            data = BasicChunker(data)

        response_context = cloudfiles.send_request(
                "PUT", self.path, data=data, headers=headers,
                data_size=data_size, chunk_size=chunk_size)
        with response_context as response:
            response.read()
        
            for name, value in response.getheaders():
                if name.lower() == 'etag':
                    if verify and value.lower() != data.last_hash.hexdigest():
                        raise RuntimeError("Bad hash")
                    self.etag = value

            self.content_length = data.last_size

    @to_error
    def update_metadata(self, metadata):
        """Update storage object metadata
        
        Metadata keys must begin with 'x-object-meta-' or
        'x-remove-object-meta-' if you're removing metadata.

        Args:
            metadata: dict of metadata to update.
        Raises:
            ResponseError, RackspaceError
        """
        valid_prefixes = ["x-object-meta-", "x-remove-object-meta-"]
        for key in metadata or {}:
            key = key.lower()
            for prefix in valid_prefixes:
                if key.startswith(prefix):
                    break
            else:
                msg = "'%s' is invalid: must start with one of: %s" % \
                        (key, valid_prefixes)
                raise ValueError(msg)

        cloudfiles = self.container.client.cloudfiles
        response_context = cloudfiles.send_request("POST", self.path, None, metadata)
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
    def copy_to(self, destination, container=None):
        """Copy object's data to another storage object
        
        Efficiently copy data from this object to another without 
        transfering data over the wire.

        Args:
            destination: StorageObject or storage object name
                of destination
            container: optional destination container if different
                from current container
        Raises:
            ResponseError, RackspaceError
        """
        if isinstance(destination, StorageObject):
            destination = destination.name

        container = container or self.container
        cloudfiles = self.container.client.cloudfiles
        headers = {"destination": "/%s/%s" % (container.name, destination) }
        response_context = cloudfiles.send_request(
                "COPY", self.path, headers=headers)
        with response_context as response:
            response.read()

    @to_error
    def copy_from(self, source, container=None):
        """Copy data to this object from another storage object
        
        Efficiently copy data to this object without
        transfering data over the wire.

        Args:
            source: StorageObject or storage object name
                of source
            container: optional destination container if different
                from current container
        Raises:
            ResponseError, RackspaceError
        """
        if isinstance(source, StorageObject):
            source = source.name

        container = container or self.container
        cloudfiles = self.container.client.cloudfiles
        headers = {"x-copy-from": "/%s/%s" % (container.name, source) }
        response_context = cloudfiles.send_request("PUT", self.path, headers=headers)
        with response_context as response:
            response.read()

    @to_error
    def purge_from_cdn(self, email=None):
        """Purge object from the CDN.

        Note that this is an extremely expensive operation and should only
        be used if absolutely necessary.

        Args:
            Email: optional email address to notify when purge is completed
        Raises:
            ResponseError, RackspaceError
        """
        cloudfiles_cdn = self.container.client.cloudfiles_cdn
        headers = {"x-purge-email": email} if email else None
        response_context = cloudfiles_cdn.send_request(
                "DELETE", self.path, headers=headers)
        with response_context as response:
            response.read()

    @to_error
    def delete_at(self, timestamp=None):
        """Delete object at specified timestamp
        
        Note: Setting timestamp to None will cancel pending delete_at.

        Args: 
            timestamp: unix timestamp to delete object at.
                If None, delete_at will be canceled.
        Raises:
            ResposneError, RackspaceError
        """
        headers = {}
        if timestamp is None:
            headers["x-remove-delete-at"] = "True"
        else:
            headers["x-delete-at"] = str(int(timestamp))

        cloudfiles = self.container.client.cloudfiles
        response_context = cloudfiles.send_request(
                "POST", self.path, None, headers)
        with response_context as response:
            response.read()

        self.delete_at_timestamp = timestamp

    @to_error
    def delete_after(self, seconds=None):
        """Delete object at after specified seconds
        
        Note: Setting seconds to None will cancel pending delete_after.

        Args: 
            seconds: number of seconds to wait before deleting object
        Raises:
            ResposneError, RackspaceError
        """
        if seconds:
            self.delete_at(time.time() + seconds)
        else:
            self.delete_at()

    @to_error
    def delete(self):
        """Delete storage object.

        Raises:
            ResposneError, RackspaceError
        """
        cloudfiles = self.container.client.cloudfiles
        response_context = cloudfiles.send_request("DELETE", self.path)
        with response_context as response:
            response.read()
