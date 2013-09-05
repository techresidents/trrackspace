import json

from trrackspace.encode import Encoder

class ServiceCatalog(object):
    @classmethod
    def from_json(cls, json):
        services = []
        for service in json:
            services.append(Service.from_json(service))
        catalog = ServiceCatalog(services)
        return catalog

    def __init__(self, services=None):
        self.services = {}
        for service in services or []:
            self.services[service.name] = service

    def __repr__(self):
        return '%s(%r)' % (self.__class__, self.services)

    def __str__(self):
        return json.dumps(self.to_json(), cls=Encoder)

    def to_json(self):
        return self.services

    def get_service(self, name):
        return self.services.get(name)

    def get_cloud_backup(self):
        return self.get_service("cloudBackup")

    def get_cloud_dns(self):
        return self.get_service("cloudDNS")

    def get_cloud_databases(self):
        return self.get_service("cloudDatabases")

    def get_cloud_files(self):
        return self.get_service("cloudFiles")

    def get_cloud_files_cdn(self):
        return self.get_service("cloudFilesCDN")

    def get_cloud_load_balancers(self):
        return self.get_service("cloudLoadBalancers")

    def get_cloud_load_balancers(self):
        return self.get_service("cloudLoadBalancers")

    def get_cloud_monitoring(self):
        return self.get_service("cloudMonitoring")

    def get_cloud_servers(self):
        return self.get_service("cloudServers")

    def get_cloud_servers_openstack(self):
        return self.get_service("cloudServersOpenStack")

class Service(object):
    @classmethod
    def from_json(cls, json):
        service = Service(
                name=json.get("name"),
                type=json.get("type"),
                endpoints=ServiceEndpoints.from_json(json.get("endpoints")))
        return service

    def __init__(self, name=None, type=None, endpoints=None):
        self.name = name
        self.type = type
        self.endpoints = endpoints

    def __repr__(self):
        return '%s(name=%r, type=%r)' % (
            self.__class__, self.name, self.type)

    def __str__(self):
        return json.dumps(self.to_json(), cls=Encoder)

    def to_json(self):
        return {
            "name": self.name,
            "type": self.type,
            "endpoints": self.endpoints.to_json()
        }

class ServiceEndpoints(object):
    @classmethod
    def from_json(cls, json):
        endpoints = []
        for endpoint in json:
            endpoints.append(ServiceEndpoint.from_json(endpoint))

        result = ServiceEndpoints(endpoints=endpoints)

        return result

    def __init__(self, endpoints=None):
        self.endpoints = {}

        for endpoint in endpoints or []:
            self.endpoints[endpoint.region] = endpoint

    def __repr__(self):
        return '%s(endpoints=%r)' % (self.__class__, self.endpoints)

    def __str__(self):
        return json.dumps(self.to_json(), cls=Encoder)

    def to_json(self):
        return self.endpoints

    def add_endpoint(self, endpoint):
        self.endpoints[endpoint.region] = endpoint

    def get_endpoint(self, region):
        return self.endpoints.get(region)

class ServiceEndpoint(object):
    @classmethod
    def from_json(cls, json):
        endpoint = ServiceEndpoint(
                region=json.get("region"),
                public_url=json.get("publicURL"),
                internal_url=json.get("internalURL"))
        return endpoint

    def __init__(self, region=None, public_url=None, internal_url=None):
        self.region = region
        self.public_url = public_url
        self.internal_url = internal_url

    def __repr__(self):
        return '%s(region=%r, public_url=%r)' % (
            self.__class__, self.region, self.public_url)

    def __str__(self):
        return json.dumps(self.to_json(), cls=Encoder)

    def to_json(self):
        return {
            "region": self.region,
            "publicURL": self.public_url,
            "internalURL": self.internal_url
        }
