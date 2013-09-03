import unittest
import time

import testbase
from trrackspace.services.identity.client import IdentityServiceClient
from trrackspace.services.identity.factory import IdentityServiceClientFactory

class TestIdentityService(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.identitysvc = IdentityServiceClient(
                username="trdev",
                password="B88mMJqh",
                timeout=5,
                debug_level=0)
    
    @classmethod
    def tearDownClass(cls):
        pass

    def test_list_users(self):
        users = self.identitysvc.list_users()

class TestIdentityServiceFacotry(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.factory = IdentityServiceClientFactory(
                username="trdev",
                password="B88mMJqh",
                timeout=5,
                debug_level=0)
    
    @classmethod
    def tearDownClass(cls):
        pass

    def test_list_users(self):
        client = self.factory.create()
        users = client.list_users()
    
if __name__ == "__main__":
    unittest.main()
