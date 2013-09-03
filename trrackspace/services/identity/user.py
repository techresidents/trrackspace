import json

class User(object):
    @classmethod
    def from_json(cls, client, json):
        user = User(client,
                id=json.get("id"),
                username=json.get("username") or json.get("name"),
                email=json.get("email"),
                enabled=json.get("enabled"),
                default_region=json.get("RAX-AUTH:defaultRegion"))
        return user

    def __init__(self, client, id=None, username=None, email=None,
            enabled=None, default_region=None):
        self.client = client
        self.id = id
        self.username = username
        self.email = email
        self.enabled = enabled
        self.default_region = default_region

    def __repr__(self):
        return '%s(id=%r, username=%r, email=%r)' % (
            self.__class__, self.id, self.username, self.email)

    def __str__(self):
        return json.dumps(self.to_json())

    def to_json(self):
        return {
            "id": self.id,
            "username": self.username,
            "email": self.email,
            "enabled": self.enabled,
            "RAX-AUTH:defaultRegion": self.default_region
        }

    def update(self):
        path = "/users/%s" % self.id
        data = json.dumps(self.to_json())
        headers = {
            "Content-type": "application/json"
        }
        response = self.client.send_request("POST", path, data, headers)
        result = json.loads(response.read())
        return result

    def get_credentials(self):
        pass

    def get_roles(self):
        pass

    def delete(self):
        path = "/users/%s" % self.id
        response = self.client.send_request("DELETE", path, None, None)
        result = response.read()
        return result
