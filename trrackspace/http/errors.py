class HttpError(Exception):
    def __init__(self, status, reason, response_data):
        self.status = status
        self.reason = reason
        self.response_data = response_data

    def __repr__(self):
        return "HttpError(%d, %s)" % (self.status, self.reason)

    def __str__(self):
        return "HttpError(%d, %s)" % (self.status, self.reason)
