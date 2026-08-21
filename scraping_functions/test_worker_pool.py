import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock

from scraping_functions.worker_pool import ThreadLocalResourcePool


class Resource:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class ThreadLocalResourcePoolTests(unittest.TestCase):
    def test_workers_never_share_stateful_resources(self):
        barrier = threading.Barrier(4)
        created = []
        creation_lock = threading.Lock()

        def factory():
            resource = Resource()
            with creation_lock:
                created.append(resource)
            return resource

        pool = ThreadLocalResourcePool(factory)

        def use_resource():
            first = pool.get()
            barrier.wait(timeout=2)
            self.assertIs(first, pool.get())
            return id(first)

        with ThreadPoolExecutor(max_workers=4) as executor:
            resource_ids = list(executor.map(lambda _index: use_resource(), range(4)))

        self.assertEqual(len(set(resource_ids)), 4)
        self.assertEqual(len(created), 4)

        pool.close_all()
        self.assertTrue(all(resource.closed for resource in created))
        with self.assertRaises(RuntimeError):
            pool.get()

    def test_close_failure_does_not_leak_other_resources(self):
        barrier = threading.Barrier(2)
        logger = Mock()
        pool = ThreadLocalResourcePool(Resource, logger=logger)

        def acquire_resource():
            resource = pool.get()
            barrier.wait(timeout=2)
            return resource

        with ThreadPoolExecutor(max_workers=2) as executor:
            resources = list(executor.map(lambda _index: acquire_resource(), range(2)))

        resources[0].close = lambda: (_ for _ in ()).throw(RuntimeError("broken"))

        pool.close_all()
        self.assertTrue(resources[1].closed)
        logger.warning.assert_called_once()


if __name__ == "__main__":
    unittest.main()
