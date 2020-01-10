# Copyright 2020-present Kensho Technologies, LLC.
"""Code for generating gRPC stubs in Kensho-approved way"""
import itertools

import grpc

from eagr.client import client_side_middleware
from eagr.client.client_tracing import wrap_grpc_client_channel


DEFAULT_CHANNEL_OPTIONS = {
    # We want to round-robin between servers
    "grpc.lb_policy_name": "round_robin",
    # These options keep the connections alive and "warm" for the next request to succeed
    # when the client does not send a constant stream of requests.
    # This is done by periodically sending keepalive pings to the server that do two things:
    # (1) reming ELBs and whatever other systems are sitting between client and server do not
    # terminate "inactive" connections and
    # (2) restoring connection to another server replica if a server on the other end goes down
    # The flags are described here https://grpc.github.io/grpc/core/group__grpc__arg__keys.html
    # A good blog post for tuning is
    # https://cs.mcgill.ca/~mxia3/2019/02/23/Using-gRPC-in-Production/
    # Client should ping server to check for liveleness this often
    # This needs to happen more frequently than the ELB timeout
    "grpc.keepalive_time_ms": 120000,
    # Keep sending keepalive pings even if no requests are outstanding
    "grpc.keepalive_permit_without_calls": 1,
    # Try to keep the channel alive forever
    "grpc.http2.max_pings_without_data": 0,
    # Minimum time between sending pings (so that we don't send them too often)
    "grpc.http2.min_time_between_pings_ms": 120000,
}


def make_grpc_client(
    client_group,
    service_name,
    service_url,
    stub_cls,
    extra_channel_options=None,
    disable_tracing=False,
    code_to_exception_class_func=None,
    num_retries=3,
    exceptions_to_retry=None,
):
    """Generate a gRPC client with appropriate middleware and options.

    The client is described by the group, name and host:port string, where group and name are used
    for metrics and logging.

    Args:
        client_group: human readable description of the client group
        service_name: human-readable name of the service for metrics/logging purposes
        service_url: host:port string to connect to
        stub_cls: stub class to instantiate
        extra_channel_options: optional dict of grpc channel options as described in
        disable_tracing: boolean, set to disable tracing for this client
        code_to_exception_class_func: optional function for translating error codes to exceptions
        num_retries: number of times to retry (retriable) exceptions
        exceptions_to_retry: optional list of retriable exceptions. (ConnectionRefusedError,)
        by default

    Returns:
        an instance of the stub class
    """
    # Start by creating a set of options
    channel_options = dict(DEFAULT_CHANNEL_OPTIONS)

    if extra_channel_options:
        channel_options.update(extra_channel_options)

    channel = grpc.insecure_channel(service_url, options=tuple(channel_options.items()))

    # We retry connection refused errors (grpc.StatusCode.UNAVAILABLE) because those are
    # generally transient
    if exceptions_to_retry is None:
        exceptions_to_retry = (ConnectionRefusedError,)

    # Note that the middlewares are applied like decorators, so the later you are in the list
    # the earlier you are applied to the call
    middlewares = [
        client_side_middleware.ClientSideExceptionCountMiddleware(client_group, service_name),
        client_side_middleware.ClientRetryingMiddlewareUnaryOutput(
            client_group, service_name, exceptions_to_retry, num_retries
        ),
        client_side_middleware.ClientExceptionTranslationMiddlewareUnaryOutput(
            client_group, service_name, code_to_exception_class_func
        ),
        client_side_middleware.ClientSideMetricsMiddleware(client_group, service_name),
    ]
    interceptors = list(
        itertools.chain.from_iterable(middleware.get_interceptors() for middleware in middlewares)
    )
    decorated_channel = grpc.intercept_channel(channel, *interceptors)
    if disable_tracing:
        traced_channel = decorated_channel
    else:
        traced_channel = wrap_grpc_client_channel(decorated_channel)

    stub = stub_cls(traced_channel)

    # Keep the pointer to the channel inside the stub so that GC does not attempt to
    # collect it in the middle of interaction
    # cf. https://blog.jeffli.me/blog/2017/08/02/keep-python-grpc-client-connection-truly-alive/
    setattr(stub, "_channel_attribute_for_no_gc", decorated_channel)

    return stub
