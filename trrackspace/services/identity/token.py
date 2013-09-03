import json

class Token(object):
    @classmethod
    def from_json(cls, json):
        token = Token(
                id=json.get("id"),
                expires=json.get("expires"))
        return token

    def __init__(self, id=None, expires=None):
        self.id = id
        self.expires = expires

    def __repr__(self):
        return '%s(id=%r, expires=%r)' % (
            self.__class__, self.id, self.expires)

    def __str__(self):
        return json.dumps(self.to_json())

    def to_json(self):
        return {
            "id": self.id,
            "expires": self.expires
        }
