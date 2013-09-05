import functools
import sys

from trhttp.errors import HttpError

class RackspaceError(Exception):
    """Base class for all errors."""
    def __init__(self, message, cause=None):
        self.message = message
        self.cause = cause
    
    def __repr__(self):
        return "%s(message=%r)" % (self.__class__, self.message)

    def __str__(self):
        return self.message

class ResponseError(RackspaceError):
    """Http response error"""
    def __init__(self, message, http_exception):
        self.message = message
        self.cause = http_exception
        self.status = http_exception.status
        self.reason = http_exception.reason
        self.response_data = http_exception.response_data
        self.response_headers = http_exception.response_headers

    def __repr__(self):
        return "%s(message=%r, status=%s, reason=%s)" % \
                (self.__class__, self.message, self.status, self.reason)

    def __str__(self):
        return "%s (status=%s, reason=%s)" % \
                (self.message, self.status, self.reason)

def to_error(message=None):
    """Decorator to map exceptions to proper errors"""
    def wrap(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except RackspaceError:
                raise
            except HttpError as e:
                raise ResponseError(message, e), None, sys.exc_info()[2]
            except Exception as e:
                msg = "%s: %s" % (message, str(e))
                raise RackspaceError(msg, e), None, sys.exc_info()[2]
        return wrapped

    #if message is a callable it means that decorator was used
    #with message argument and message is actually the func
    #we need to decorate
    if callable(message):
        func = message
        message = "error in %s" % func.__name__
        return wrap(func)
    #decorator was used with message
    else:
        return wrap

