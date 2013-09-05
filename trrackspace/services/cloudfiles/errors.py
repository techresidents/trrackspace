from trrackspace.errors import RackspaceError

class NoSuchContainer(RackspaceError):
    def __init__(self, name):
        message = "no such container: %s" % name
        super(NoSuchContainer, self).__init__(message)

class NoSuchObject(RackspaceError):
    def __init__(self, name):
        message = "no such object: %s" % name
        super(NoSuchObject, self).__init__(message)

class ContainerNotEmpty(RackspaceError):
    def __init__(self):
        message = "container not empty"
        super(ContainerNotEmpty, self).__init__(message)

class ExtractArchiveError(RackspaceError):
    def __init__(self, result):
        self.result = result
        self.errors = result.get("Errors")
        message = "extract archive failed: %s" % self.errors
        super(ExtractArchiveError, self).__init__(message)
