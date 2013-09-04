import hashlib
import os
import time
import unittest
import urllib

import testbase

from trrackspace.services.cloudfiles.errors import NoSuchContainer, \
        NoSuchObject, ContainerNotEmpty

from trrackspace.services.cloudfiles.client import CloudfilesClient
from trrackspace.services.cloudfiles.factory import CloudfilesClientFactory
from trrackspace.services.cloudfiles.storage_object import StorageObject

class TestCloudfiles(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.cloudfiles = CloudfilesClient(
                username="trdev",
                password="B88mMJqh",
                timeout=30,
                retries=2,
                servicenet=False,
                debug_level=0)

        cls.container_name = "tr_unittest"    
        cls.container = cls.cloudfiles.create_container(cls.container_name)
    
    @classmethod
    def tearDownClass(cls):
        cls.container.delete_all_objects()
        cls.container.delete()

    def test_list_containers(self):
        containers = self.cloudfiles.list_containers()
        self.assertTrue(len(containers))

        container = containers[0]
        self.assertTrue(isinstance(container, dict))
        self.assertTrue("count" in container)
        self.assertTrue("bytes" in container)
        self.assertTrue("name" in container)

    def test_create_container(self):
        container_name = "trunittest_tmp_%s" % (int(time.time()))
        container = self.cloudfiles.create_container(container_name)
        self.assertEqual(container.name, container_name)
        self.assertEqual(container.count, 0)
        self.assertEqual(container.size, 0)
        self.assertEqual(container.cdn_enabled, False)
        self.assertEqual(container.cdn_uri, None)
        self.assertEqual(container.cdn_ssl_uri, None)
        self.assertEqual(container.cdn_streaming_uri, None)
        container.delete()

    def test_get_container(self):
        container = self.cloudfiles.get_container(self.container_name)
        self.assertEqual(container.name, self.container_name)

        with self.assertRaises(NoSuchContainer):
            self.cloudfiles.get_container("blahblahblah")

class TestCloudfilesContainer(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.cloudfiles = CloudfilesClient(
                username="trdev",
                password="B88mMJqh",
                timeout=30,
                retries=2,
                servicenet=False,
                debug_level=0)

        cls.container_name = "tr_unittest"    
        cls.container = cls.cloudfiles.create_container(cls.container_name)
    
    @classmethod
    def tearDownClass(cls):
        cls.container.delete_all_objects()
        cls.container.delete()

    def test_list(self):
        object_names = ["a.txt", "tmp/b.txt", "tmp/c.txt", "tmp2/d.txt"]
        for name in object_names:
            object = self.container.create_object(name)
            object.write("test")
        
        names = self.container.list()
        self.assertListEqual(names, object_names)

        names = self.container.list(delimiter="/")
        self.assertListEqual(names, ["a.txt", "tmp/", "tmp2/"])

        names = self.container.list(prefix="tmp/", delimiter="/")
        self.assertListEqual(names, ["tmp/b.txt", "tmp/c.txt"])

        self.container.delete_objects(object_names)

    def test_list_objects(self):
        object_names = ["a.txt", "b.txt", "c.txt"]
        for name in object_names:
            object = self.container.create_object(name)
            object.write("test")
        
        objects = self.container.list_objects()
        self.assertEqual(len(objects), len(object_names))
        self.assertListEqual(object_names, [o["name"] for o in objects])

        obj = objects[0]
        self.assertTrue(isinstance(obj, dict))
        self.assertTrue("name" in obj)
        self.assertTrue("last_modified" in obj)
        self.assertTrue("hash" in obj)
        self.assertTrue("content_type" in obj)

        objects = self.container.list_objects(limit=1)
        self.assertEqual(len(objects), 1)

        self.container.delete_objects(object_names)

    def test_list_dir_objects(self):
        object_names = ["a.txt", "tmp/b.txt", "tmp/c.txt", "tmp2/d.txt"]
        for name in object_names:
            object = self.container.create_object(name)
            object.write("test")
        
        objects = self.container.list_objects()
        self.assertListEqual(object_names, [o.get("name") for o in objects])

        objects = self.container.list_objects(delimiter="/")
        self.assertListEqual(["a.txt", None, None], [o.get("name") for o in objects])

        objects = self.container.list_objects(prefix="tmp/", delimiter="/")
        self.assertListEqual(["tmp/b.txt", "tmp/c.txt"], [o.get("name") for o in objects])

        self.container.delete_objects(object_names)

    def test_list_all_objects(self):
        object_names = ["a.txt", "b.txt", "c.txt"]
        for name in object_names:
            object = self.container.create_object(name)
            object.write("test")
        
        objects = [o for o in self.container.list_all_objects()]
        self.assertEqual(len(objects), len(object_names))
        self.assertListEqual(object_names, [o["name"] for o in objects])

        objects = [o for o in self.container.list_all_objects(batch_size=1)]
        self.assertEqual(len(objects), len(object_names))
        self.assertListEqual(object_names, [o["name"] for o in objects])

        objects = [o for o in self.container.list_all_objects(batch_size=2)]
        self.assertEqual(len(objects), len(object_names))
        self.assertListEqual(object_names, [o["name"] for o in objects])

        self.container.delete_objects(object_names)

    
    def test_create_object(self):
        object_name = "create.txt"
        object_data = "data"
        obj = self.container.create_object(object_name)
        
        #object should not exist until we write to it
        obj.write(object_data)

        obj2 = self.container.get_object(object_name)
        
        self.assertEqual(obj.read(), object_data)
        self.assertEqual(obj2.read(), object_data)

        obj.delete()

    def test_get_object(self):
        object_name = "get_object.txt"
        object_data = "data"

        with self.assertRaises(NoSuchObject):
            self.container.get_object(object_name)

        obj = self.container.create_object(object_name)
        obj.write(object_data)

        obj2 = self.container.get_object(object_name)
        self.assertEqual(obj.read(), object_data)
        self.assertEqual(obj2.read(), object_data)

        obj.delete()

    def test_delete_object(self):
        object_name = "delete_object.txt"
        object_data = "data"
        obj = self.container.create_object(object_name)
        obj.write(object_data)

        self.container.delete_object(object_name)

        with self.assertRaises(NoSuchObject):
            self.container.delete_object(object_name)

    def test_delete_objects(self):
        object_names = ["a.txt", "b.txt", "c.txt"]
        object_data = "data"
        for object_name in object_names:
            obj = self.container.create_object(object_name)
            obj.write(object_data)
        
        #delete first two objects
        self.container.delete_objects(object_names[:2])

        for object_name in object_names[:2]:
            with self.assertRaises(NoSuchObject):
                self.container.get_object(object_name)

        obj = self.container.get_object(object_names[-1])
        self.assertEqual(obj.read(), object_data)

        self.container.delete_objects(object_names[-1:])
        with self.assertRaises(NoSuchObject):
            self.container.get_object(object_names[-1])

    
    def test_delete_all_objects(self):
        object_names = ["a.txt", "b.txt", "c.txt"]
        object_data = "data"
        for object_name in object_names:
            obj = self.container.create_object(object_name)
            obj.write(object_data)
        
        self.container.delete_all_objects(batch_size=2)

        for object_name in object_names:
            with self.assertRaises(NoSuchObject):
                self.container.get_object(object_name)
    
    def test_delete(self):
        container_name = "trunittest_delete"
        container = self.cloudfiles.create_container(container_name)

        object_name = "delete.txt"
        obj = container.create_object(object_name)
        obj.write("data")

        container = self.cloudfiles.get_container(container_name)

        with self.assertRaises(ContainerNotEmpty):
            container.delete()
        
        container.delete_all_objects()
        container.delete()

        with self.assertRaises(NoSuchContainer):
            self.cloudfiles.get_container(container_name)

        with self.assertRaises(NoSuchContainer):
            container.delete()

    def test_update_metadata(self):
        key = "x-container-meta-unittest"
        remove_key = "x-remove-container-meta-unittest"
        value = "unittest"
        metadata = {}
        metadata[key] = value

        self.assertTrue(key not in self.container.metadata)
        self.container.update_metadata(metadata)
        self.assertEqual(self.container.metadata[key], value)
        
        container = self.cloudfiles.get_container(self.container_name)
        self.assertEqual(self.container.metadata[key], value)

        metadata = {}
        metadata[remove_key] = True
        self.container.update_metadata(metadata)
        self.assertTrue(key not in self.container.metadata)
        
        container = self.cloudfiles.get_container(self.container_name)
        self.assertTrue(key not in container.metadata)

        metadata = { "invalid-key": "test" }
        with self.assertRaises(Exception):
            self.container.update_metadata(metadata)
    
    def test_object_versioning(self):
        backup_container = self.cloudfiles.create_container("trunittest_backup")
        self.container.enable_object_versioning(backup_container)
        self.container.disable_object_versioning()
        
        object_name = "object_versioning.txt"
        obj = self.container.create_object(object_name)
        obj.write("data1")
        obj.write("data2")
        self.assertEqual(len(backup_container.list()), 0)

        self.container.enable_object_versioning(backup_container)
        obj.write("data3")
        obj.write("data4")
        obj.write("data5")
        
        self.assertEqual(obj.read(), "data5")
        self.assertEqual(len(backup_container.list()), 3)
        obj.delete()
        self.assertEqual(obj.read(), "data4")
        self.assertEqual(len(backup_container.list()), 2)
        obj.delete()
        self.assertEqual(obj.read(), "data3")
        self.assertEqual(len(backup_container.list()), 1)
        obj.delete()
        self.assertEqual(obj.read(), "data2")
        self.assertEqual(len(backup_container.list()), 0)
        obj.delete()

        with self.assertRaises(NoSuchObject):
            obj.load()

        backup_container.delete()

    def test_log_retention(self):
        self.assertEqual(self.container.cdn_log_retention, False)

        self.container.enable_log_retention()
        container = self.cloudfiles.get_container(self.container_name)
        self.assertEqual(container.cdn_log_retention, True)
        self.assertEqual(self.container.cdn_log_retention, True)

        self.container.disable_log_retention()
        container = self.cloudfiles.get_container(self.container_name)
        self.assertEqual(container.cdn_log_retention, False)
        self.assertEqual(self.container.cdn_log_retention, False)

    
    def test_cdn(self):
        try:
            container_name = "tr_unittest_cdn_%s" % str(time.time())
            container = self.cloudfiles.create_container(container_name)
            self.assertFalse(container.cdn_enabled)

            data = "cdn data"
            obj = container.create_object("cdn.txt")
            obj.write(data)
            self.assertIsNone(obj.cdn_uri)
            self.assertIsNone(obj.cdn_ssl_uri)
            self.assertIsNone(obj.cdn_streaming_uri)

            container.enable_cdn()
            self.assertTrue(container.cdn_enabled)
            self.assertIsNotNone(obj.cdn_uri)
            self.assertIsNotNone(obj.cdn_ssl_uri)
            self.assertIsNotNone(obj.cdn_streaming_uri)

            response = urllib.urlopen(obj.cdn_uri)
            self.assertEqual(response.read(), data)

            container.disable_cdn()
            self.assertFalse(container.cdn_enabled)
            self.assertIsNone(obj.cdn_uri)
            self.assertIsNone(obj.cdn_ssl_uri)
            self.assertIsNone(obj.cdn_streaming_uri)

        finally:
            container.delete_all_objects()
            container.delete()
    
    def test_extract_archive(self):
        path = os.path.join(os.path.dirname(__file__), "data/cloudfiles_archive.tar.gz")
        result = self.container.extract_archive(path)
        self.assertEqual(result.get("Number Files Created"), 3)
        files = ["a.txt", "tmp/b.txt", "tmp/c.txt"]
        self.assertListEqual(self.container.list(), files)
        self.container.delete_all_objects()

class TestCloudfilesStorageObject(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):

        cls.username = "trdev"
        cls.password = "B88mMJqh"
        cls.timeout = 30
        cls.retries = 2
        cls.servicenet = False
        cls.debug_level = 0

        cls.cloudfiles = CloudfilesClient(
                username=cls.username,
                password=cls.password,
                timeout=cls.timeout,
                retries = cls.retries,
                servicenet=cls.servicenet,
                debug_level=cls.debug_level)

        cls.container_name = "tr_unittest"    
        cls.container = cls.cloudfiles.create_container(cls.container_name)
    
    @classmethod
    def tearDownClass(cls):
        cls.container.delete_all_objects()
        cls.container.delete()

    def test_constructor(self):
        obj = StorageObject(self.container, "test.txt")
        self.assertEqual(obj.content_type.lower(), "text/plain")

        with self.assertRaises(NoSuchObject):
            StorageObject(self.container, "test.txt", exists=True)
        
        object_data = "data"
        obj.write(object_data)

        obj = StorageObject(self.container, "test.txt", exists=True)
        self.assertEqual(obj.content_type.lower(), "text/plain")
        self.assertEqual(obj.content_length, len(object_data))
        self.assertEqual(obj.etag.lower(), hashlib.md5(object_data).hexdigest())

        obj.delete()

    def test_temp_url(self):
        obj = self.container.create_object("test.txt")
        object_data = "data"
        obj.write(object_data)

        url = obj.temp_url("GET", 10)
        response = urllib.urlopen(url)
        self.assertEqual(response.read(), object_data)

        time.sleep(10)
        with self.assertRaises(IOError):
            urllib.urlopen(url)

        obj.delete()

    def test_read(self):
        obj = self.container.create_object("test.txt")
        object_data = "data"
        obj.write(object_data)

        self.assertEqual(obj.read(), object_data)
        self.assertEqual(obj.read(size=2), object_data[:2])
        self.assertEqual(obj.read(size=2, offset=2), object_data[2:4])
        self.assertEqual(obj.read(offset=3), object_data[3:])

        obj.delete()

    def test_read_output(self):
        obj = self.container.create_object("test.txt")
        object_data = "data"
        obj.write(object_data)
        
        cloudfiles = CloudfilesClient(
                username=self.username,
                password=self.password,
                timeout=self.timeout,
                servicenet=self.servicenet,
                debug_level=self.debug_level)
        
        #note that will only work if data size is less than
        #output_chunk_size since cloudfiles does not support
        #writes with offsets
        output_container = cloudfiles.get_container(self.container_name)
        output_object = output_container.create_object("test_output.txt")
        obj.read(output=output_object)
        self.assertEqual(output_object.read(), object_data)

        #test chunking relying on the fact the last chunk
        #written to object will be the result
        output_container = cloudfiles.get_container(self.container_name)
        output_object = output_container.create_object("test_output_last.txt")
        obj.read(output=output_object, output_chunk_size=2)
        self.assertEqual(output_object.read(), object_data[-2:])

        self.container.delete_all_objects()

    def test_chunks(self):
        obj = self.container.create_object("test.txt")
        object_data = "abcdefghijklmnopqrstuvwxyz"
        obj.write(object_data)
        
        chunks = [c for c in obj.chunks()]
        self.assertListEqual([object_data], chunks)

        chunks = [c for c in obj.chunks(size=13)]
        self.assertListEqual([object_data[:13]], chunks)

        chunks = [c for c in obj.chunks(size=13, offset=13)]
        self.assertListEqual([object_data[13:]], chunks)

        chunks = [c for c in obj.chunks(chunk_size=13)]
        self.assertListEqual([object_data[:13], object_data[13:]], chunks)

        chunks = [c for c in obj.chunks(offset=13, chunk_size=13)]
        self.assertListEqual([object_data[13:]], chunks)

        chunks = [c for c in obj.chunks(offset=13, chunk_size=12)]
        self.assertListEqual([object_data[13:25], object_data[25:]], chunks)

        obj.delete()

    def test_write(self):
        obj = self.container.create_object("test.txt")
        object_data = "data"
        obj.write(object_data)

        self.assertEqual(obj.etag.lower(), hashlib.md5(object_data).hexdigest())
        self.assertEqual(obj.read(), object_data)

        obj.write(object_data[:3], chunk_size=1)
        self.assertEqual(obj.read(), object_data[:3])
        self.assertEqual(obj.etag.lower(), hashlib.md5(object_data[:3]).hexdigest())

        obj.write(object_data, verify=False)
        self.assertEqual(obj.read(), object_data)
        self.assertEqual(obj.etag.lower(), hashlib.md5(object_data).hexdigest())

        self.container.delete_all_objects()

    def test_write_file_like(self):
        obj = self.container.create_object("test.txt")
        object_data = "data"
        obj.write(object_data)

        cloudfiles = CloudfilesClient(
                username=self.username,
                password=self.password,
                timeout=self.timeout,
                servicenet=self.servicenet,
                debug_level=self.debug_level)

        container = cloudfiles.get_container(self.container_name)
        new_obj = container.create_object("test_copy.txt")
        new_obj.write(obj)
        self.assertEqual(new_obj.read(), object_data)

        new_obj.write(obj, chunk_size=1)
        self.assertEqual(new_obj.read(), object_data)

        self.container.delete_all_objects()

    def test_update_metadata(self):
        key = "x-object-meta-unittest"
        remove_key = "x-remove-object-meta-unittest"
        value = "unittest"
        metadata = {}
        metadata[key] = value

        object_name = "test.txt"
        obj = self.container.create_object(object_name)
        object_data = "data"
        obj.write(object_data)

        self.assertTrue(key not in obj.metadata)
        obj.update_metadata(metadata)
        self.assertEqual(obj.metadata[key], value)
        
        obj = self.container.get_object(object_name)
        self.assertEqual(obj.metadata[key], value)

        metadata = {}
        metadata[remove_key] = True
        obj.update_metadata(metadata)
        self.assertTrue(key not in obj.metadata)
        
        obj = self.container.get_object(object_name)
        self.assertTrue(key not in obj.metadata)

        metadata = { "invalid-key": "test" }
        with self.assertRaises(Exception):
            obj.update_metadata(metadata)

        obj.delete()

    def test_update_cors(self):
        cors = {
            "access-control-allow-origin": "*",
            "access-control-max-age": "3600"
        }
        object_name = "test_cors.txt"
        obj = self.container.create_object(object_name)
        object_data = "data"
        obj.write(object_data)

        self.assertEqual(len(obj.cors), 0)
        obj.update_cors(cors)
        self.assertEqual(obj.cors["access-control-allow-origin"], "*")
        self.assertEqual(obj.cors["access-control-max-age"], "3600")
        
        obj = self.container.get_object(object_name)
        self.assertEqual(obj.cors["access-control-allow-origin"], "*")
        self.assertEqual(obj.cors["access-control-max-age"], "3600")
        obj.delete()

        #test creation with CORS headers
        object_name = "test_cors_new.txt"
        obj = self.container.create_object(object_name, cors=cors)
        self.assertEqual(obj.cors["access-control-allow-origin"], "*")
        self.assertEqual(obj.cors["access-control-max-age"], "3600")
        obj.write(object_data)

        obj = self.container.get_object(object_name)
        self.assertEqual(obj.cors["access-control-allow-origin"], "*")
        self.assertEqual(obj.cors["access-control-max-age"], "3600")
        
        #test invalid header
        cors = { "invalid-key": "test" }
        with self.assertRaises(Exception):
            obj.update_cors(cors)

        obj.delete()
        

    def test_copy_to(self):
        object_name = "test.txt"
        obj = self.container.create_object(object_name)
        object_data = "data"
        obj.write(object_data)

        obj.copy_to("test2.txt")
        obj = self.container.get_object("test2.txt")
        self.assertEqual(obj.read(), object_data)

        self.container.delete_all_objects()

    def test_copy_from(self):
        object_name = "test.txt"
        obj = self.container.create_object(object_name)
        object_data = "data"
        obj.write(object_data)
        
        new_obj = self.container.create_object("test2.txt")
        new_obj.copy_from(obj)
        self.assertEqual(obj.read(), object_data)

        self.container.delete_all_objects()

    def test_delete_at(self):
        object_name = "test.txt"
        obj = self.container.create_object(object_name)
        object_data = "data"
        obj.write(object_data)
        
        timestamp = int(time.time() + 10)
        obj.delete_at(timestamp)
        self.assertEqual(obj.delete_at_timestamp, timestamp)
        self.assertEqual(obj.read(), object_data)
        obj = self.container.get_object(object_name)
        self.assertEqual(obj.delete_at_timestamp, timestamp)
        time.sleep(11)

        with self.assertRaises(NoSuchObject):
            self.container.get_object(object_name)

    def test_delete_at_cancel(self):
        object_name = "test.txt"
        obj = self.container.create_object(object_name)
        object_data = "data"
        obj.write(object_data)
        
        timestamp = int(time.time() + 10)
        obj.delete_at(timestamp)
        self.assertEqual(obj.delete_at_timestamp, timestamp)
        self.assertEqual(obj.read(), object_data)
        obj = self.container.get_object(object_name)
        self.assertEqual(obj.delete_at_timestamp, timestamp)
        obj.delete_at()
        self.assertIsNone(obj.delete_at_timestamp)
        time.sleep(11)

        self.assertEqual(obj.read(), object_data)

    def test_delete_after(self):
        object_name = "test.txt"
        obj = self.container.create_object(object_name)
        object_data = "data"
        obj.write(object_data)
        
        obj.delete_after(10)
        self.assertIsNotNone(obj.delete_at_timestamp)
        self.assertEqual(obj.read(), object_data)
        obj = self.container.get_object(object_name)
        self.assertIsNotNone(obj.delete_at_timestamp)
        time.sleep(11)

        with self.assertRaises(NoSuchObject):
            self.container.get_object(object_name)

    def test_delete_after_cancel(self):
        object_name = "test.txt"
        obj = self.container.create_object(object_name)
        object_data = "data"
        obj.write(object_data)
        
        obj.delete_after(10)
        self.assertIsNotNone(obj.delete_at_timestamp)
        self.assertEqual(obj.read(), object_data)
        obj = self.container.get_object(object_name)
        self.assertIsNotNone(obj.delete_at_timestamp)
        obj.delete_after()
        self.assertIsNone(obj.delete_at_timestamp)
        time.sleep(11)

        self.assertEqual(obj.read(), object_data)

    
    def test_delete(self):
        object_name = "test.txt"
        obj = self.container.create_object(object_name)
        object_data = "data"
        obj.write(object_data)

        obj = self.container.get_object(object_name)
        self.assertEqual(obj.read(), object_data)

        obj.delete()
        with self.assertRaises(NoSuchObject):
            self.container.get_object(object_name)


class TestCloudfilesConnection(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.cloudfiles = CloudfilesClient(
                username="trdev",
                password="B88mMJqh",
                timeout=30,
                retries=2,
                servicenet=False,
                debug_level=0)

        cls.container_name = "tr_unittest"    
        cls.container = cls.cloudfiles.create_container(cls.container_name)
    
    @classmethod
    def tearDownClass(cls):
        cls.container.delete_all_objects()
        cls.container.delete()

    def test_persistent_connection(self):
        object_names = ["a.txt", "tmp/b.txt", "tmp/c.txt", "tmp2/d.txt"]
        for name in object_names:
            object = self.container.create_object(name)
            object.write("test")
        
        names = self.container.list()
        self.assertListEqual(names, object_names)

        time.sleep(30)

        names = self.container.list()
        self.assertListEqual(names, object_names)

        self.container.delete_objects(object_names)


class TestCloudfilesFactory(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.factory = CloudfilesClientFactory(
                username="trdev",
                password="B88mMJqh",
                timeout=30,
                retries=2,
                servicenet=False,
                debug_level=0)

    @classmethod
    def tearDownClass(cls):
        pass

    def test_create(self):
        client = self.factory.create()
        client.list_containers()


if __name__ == "__main__":
    unittest.main()
