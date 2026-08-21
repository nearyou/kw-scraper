"""Thread-local resource ownership for concurrent scraper workers."""

import logging
import threading


class ThreadLocalResourcePool:
    """Create one resource per worker thread and close every resource safely.

    Browser objects are stateful and must never be shared between concurrent
    workers. The registry is protected only while resources are registered or
    drained; resource use remains independent and does not serialize workers.
    """

    def __init__(self, factory, *, closer=None, logger=None):
        self.factory = factory
        self.closer = closer or (lambda resource: resource.close())
        self.logger = logger or logging.getLogger(__name__)
        self._local = threading.local()
        self._resources = {}
        self._lock = threading.Lock()
        self._closed = False

    def get(self):
        resource = getattr(self._local, "resource", None)
        if resource is not None:
            return resource

        with self._lock:
            if self._closed:
                raise RuntimeError("worker resource pool is closed")
        resource = self.factory()
        thread_id = threading.get_ident()
        with self._lock:
            if self._closed:
                self.closer(resource)
                raise RuntimeError("worker resource pool closed during setup")
            self._resources[thread_id] = resource
        self._local.resource = resource
        return resource

    def close_all(self):
        """Close each registered resource once, even if one close fails."""
        with self._lock:
            self._closed = True
            resources = list(self._resources.items())
            self._resources.clear()

        for thread_id, resource in resources:
            try:
                self.closer(resource)
            except Exception as error:
                self.logger.warning(
                    "worker_resource_close_failed",
                    extra={
                        "context": {
                            "thread_id": thread_id,
                            "error_type": type(error).__name__,
                        }
                    },
                )
