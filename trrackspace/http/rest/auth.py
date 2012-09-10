import abc

class RestAuthenticator(object):
    __metaclass__ = abc.ABCMeta
    
    @abc.abstractmethod
    def authenticate():
        return
