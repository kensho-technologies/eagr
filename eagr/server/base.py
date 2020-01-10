# Copyright 2020-present Kensho Technologies, LLC.
from concurrent import futures
from contextlib import contextmanager
import logging
import warnings

import grpc
from grpc_reflection.v1alpha.reflection import enable_server_reflection
import prometheus_client


GRPC_REGISTRAR_ATTRIBUTE = "_REGISTRAR"
GRPC_TRACING_ATTRIBUTE = "_TRACING_ENABLED"
GRPC_GRACE_PERIOD = 10  # seconds

logger = logging.getLogger(__name__)


class GRPCBase(object):
    """Base class for simple GRPC servers.  Mostly a way to apply the metaclass"""

    _REGISTRAR = None  # Dont break the construction of this class with the assert

    def __init__(self):
        """Assert that the _REGISTRAR attribute exists"""
        super(GRPCBase, self).__init__()
        if not hasattr(self, GRPC_REGISTRAR_ATTRIBUTE):
            raise KeyError("GRPC implementation must define %s" % GRPC_REGISTRAR_ATTRIBUTE)


@contextmanager
def run_grpc_servers(
    servers,
    grpc_interface="0.0.0.0",
    grpc_port=7999,
    metrics_port=None,
    thread_pool=None,
    middlewares=None,
    grpc_server_options=None,
    enable_reflection_for_services=None,
    key_cert_pairs=None,
):
    """Run a bunch of GRPC servers

    Args:
        servers: Iterable of GRPCBase instances that will be exposed
        grpc_interface: Network interface to which grpc will be bound.  Probably shouldn't
                        be changed but if you only want to expose a service locally allows
                        for 127.0.0.1
        grpc_port: Port for GRPC requests (HTTP/2)
        metrics_port: Port for metrics (HTTP/1.1). Optional, must specify to enable metrics
        thread_pool: Thread pool for network requests.  Optional but the default is
                     unlikely to work for all use cases.
        middlewares: List of GRPCMiddleware objects
        grpc_server_options: an object that contains options directly passed to the grpc.server call
        enable_reflection_for_services: optional list of services for which to enable reflection
        key_cert_pairs: optional list of PEM encoded (key, cert_chain) pairs for TLS use
    """
    if thread_pool is None:
        warnings.warn("No thread pool specific - defaulting to 10 workers!")
        thread_pool = futures.ThreadPoolExecutor(max_workers=10)

    if middlewares is None:
        middlewares = []

    interceptors = []
    for middleware in middlewares:
        interceptors.extend(middleware.get_interceptors())

    grpc_server = grpc.server(thread_pool, interceptors=interceptors, options=grpc_server_options)

    try:
        for server in servers:
            getattr(server, GRPC_REGISTRAR_ATTRIBUTE)(grpc_server)
        grpc_address = grpc_interface + ":" + str(grpc_port)
        if key_cert_pairs:
            server_creds = grpc.ssl_server_credentials(key_cert_pairs)
            grpc_server.add_secure_port(grpc_address, server_creds)
        else:
            grpc_server.add_insecure_port(grpc_address)
        if enable_reflection_for_services is not None:
            enable_server_reflection(enable_reflection_for_services, grpc_server)
        grpc_server.start()
        if metrics_port is not None:
            prometheus_client.start_http_server(metrics_port)
        yield None
    finally:
        event = grpc_server.stop(GRPC_GRACE_PERIOD)
        event.wait(GRPC_GRACE_PERIOD)
