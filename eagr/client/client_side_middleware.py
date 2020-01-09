# Copyright 2020-present Kensho Technologies, LLC.
"""Implementing client-side grpc interceptors"""
import functools
import json

import backoff
import grpc
import prometheus_client


CLIENTSIDE_METRICS_HISTO = prometheus_client.Histogram(
    "clientside_grpc_endpoint",
    "Response time histogram for grpc endpoints from the client-side",
    labelnames=("client_name", "server_name", "service", "endpoint"),
)
CLIENTSIDE_ERROR_COUNTER = prometheus_client.Counter(
    "clientside_grpc_endpoint_error",
    "Clientside exception counts for grpc methods",
    labelnames=("client_name", "server_name", "service", "endpoint", "exception"),
)

GRPC_RENDEZVOUS_ERROR = "_Rendezvous"


def get_service_and_method_from_url(method_url):
    """Extract service and method names from the method url string.

    Returns strings that are applicable as prometheus metrics and/or labels.

    Args:
        method_url: string

    Returns:
        tuple(service_name, method_name)
    """
    name_parts = method_url.split("/")
    if len(name_parts) != 3 or name_parts[0] != "" or name_parts[1] == "" or name_parts[2] == "":
        raise AssertionError("Invalid method name: {}".format(method_url))

    return (name_parts[1].replace(".", "_"), name_parts[2].replace(".", "_"))


class GRPCClientGeneralInterceptor(
    grpc.UnaryUnaryClientInterceptor,
    grpc.StreamUnaryClientInterceptor,
    grpc.UnaryStreamClientInterceptor,
    grpc.StreamStreamClientInterceptor,
):
    """General GRPC client interceptor that intercepts all functions."""

    def __init__(self, decorator_fn):
        """Initialize interceptor with a factory function producing decorators."""
        super(GRPCClientGeneralInterceptor, self).__init__()
        self._decorator_fn = decorator_fn

    def _intercept_call(self, continuation, client_call_details, request_or_iterator):
        """Interceptor implementation."""
        metadata = _get_metadata_map_from_client_details(client_call_details)
        decorator = self._decorator_fn(client_call_details.method, metadata)
        if not decorator:
            handler = continuation
        else:
            handler = decorator(continuation)

        return handler(client_call_details, request_or_iterator)

    def intercept_unary_unary(self, continuation, client_call_details, request):
        """Intercept unary-unary."""
        return self._intercept_call(continuation, client_call_details, request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        """Intercept stream-unary."""
        return self._intercept_call(continuation, client_call_details, request_iterator)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        """Intercept unary-stream."""
        return self._intercept_call(continuation, client_call_details, request)

    def intercept_stream_stream(self, continuation, client_call_details, request_iterator):
        """Intercept stream-stream."""
        return self._intercept_call(continuation, client_call_details, request_iterator)


class GRPCClientUnaryOutputInterceptor(
    grpc.UnaryUnaryClientInterceptor, grpc.StreamUnaryClientInterceptor
):
    """GRPC interceptor that makes intercepts only unary-output grpcs."""

    def __init__(self, decorator_fn):
        """Initialize interceptor with a factory function producing decorators."""
        super(GRPCClientUnaryOutputInterceptor, self).__init__()
        self._decorator_fn = decorator_fn

    def _intercept_call(self, continuation, client_call_details, request_or_iterator):
        """Interceptor implementation"""
        metadata = _get_metadata_map_from_client_details(client_call_details)
        decorator = self._decorator_fn(client_call_details.method, metadata)
        if not decorator:
            handler = continuation
        else:
            handler = decorator(continuation)

        return handler(client_call_details, request_or_iterator)

    def intercept_unary_unary(self, continuation, client_call_details, request):
        """Intercept unary-unary."""
        return self._intercept_call(continuation, client_call_details, request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        """Intercept stream-unary."""
        return self._intercept_call(continuation, client_call_details, request_iterator)


class GRPCClientMiddleware(object):
    """Base class for GRPC client-side middleware.

    GRPCMiddleware implementations must provide a get_decorator method:

    # def get_decorator(self, method_name, metadata)


    Which takes a string method name, and dict of rpc leading metadata and
    returns a decorator that can be applied to the underlying rpc method.
    Additionally:
      __init__ is guaranteed to be called before the server is started.
      get_interceptors(self) will be called to retrieve all GRPC interceptors
        necessary for the middleware.  Users may extend this method to include
        additional interceptors.
    """

    def __init__(self, client_label, server_label, interceptor_class):
        """Initialize"""
        super(GRPCClientMiddleware, self).__init__()
        self._server_label = server_label
        self._client_label = client_label
        self._interceptor_class = interceptor_class

    @property
    def server_label(self):
        """Get server label."""
        return self._server_label

    @property
    def client_label(self):
        """Get client label."""
        return self._client_label

    def get_interceptors(self):
        """Get a list of interceptors needed by the middleware."""
        return [self._interceptor_class(self.get_decorator)]


class ClientSideMetricsMiddleware(GRPCClientMiddleware):
    """GRPC middleware that captures prometheus metrics."""

    def __init__(self, client_label, server_label):
        """Initialize"""
        super(ClientSideMetricsMiddleware, self).__init__(
            client_label, server_label, GRPCClientGeneralInterceptor
        )

    class Timer(object):
        """Decorator that wraps a function in a prometheus histogram."""

        def __init__(self, histogram):
            """Initializes with the histogram object."""
            self._histogram = histogram

        def __call__(self, fn):
            """Wrap a method with a histogram."""

            @functools.wraps(fn)
            def wrap(request, context):
                """Inner wrapper."""
                with self._histogram.time():
                    return fn(request, context)

            return wrap

    def get_decorator(self, method_name, _):
        """Normalize metric name and return decorator that captures metrics."""
        service_label, endpoint_label = get_service_and_method_from_url(method_name)
        return self.Timer(
            CLIENTSIDE_METRICS_HISTO.labels(
                client_name=self.client_label,
                server_name=self.server_label,
                service=service_label,
                endpoint=endpoint_label,
            )
        )


class ClientSideExceptionCountMiddleware(GRPCClientMiddleware):
    """GRPC middleware that captures prometheus metrics for unary outputs."""

    def __init__(self, client_label, server_label):
        """Initialize"""
        super(ClientSideExceptionCountMiddleware, self).__init__(
            client_label, server_label, GRPCClientUnaryOutputInterceptor
        )

    class Counter(object):
        """Decorator that wraps a function in a exception counter."""

        def __init__(self, counter, client_name, server_name, service, endpoint):
            """Initializes with the counter object."""
            self._counter = counter
            self._client_name = client_name
            self._server_name = server_name
            self._service = service
            self._endpoint = endpoint

        def __call__(self, fn):
            """Wrap a method with an exception counter."""

            @functools.wraps(fn)
            def wrap(request, context):
                """Inner wrapper."""
                r = fn(request, context)
                if r.exception():
                    # If we get a Rendezvous error, we want some more information about the type
                    # of error we are getting. For example, a GRPC timeout error will be labelled as
                    # exception "_Rendezvous: <StatusCode.DEADLINE_EXCEEDED: 4>". All errors can be
                    # found at https://grpc.github.io/grpc/python/grpc.html#grpc-status-code
                    if type(r.exception()).__name__ == GRPC_RENDEZVOUS_ERROR:
                        exception = GRPC_RENDEZVOUS_ERROR + ": " + repr(r.exception().code())
                    # No guarantees of status code for other errors--only report error type.
                    else:
                        exception = type(r.exception()).__name__
                    self._counter.labels(
                        client_name=self._client_name,
                        server_name=self._server_name,
                        service=self._service,
                        endpoint=self._endpoint,
                        exception=exception,
                    ).inc()
                return r

            return wrap

    def get_decorator(self, method_name, _):
        """Normalize method name and return decorator that captures exceptions"""
        service_label, endpoint_label = get_service_and_method_from_url(method_name)
        return self.Counter(
            CLIENTSIDE_ERROR_COUNTER,
            self.client_label,
            self.server_label,
            service_label,
            endpoint_label,
        )


class ClientExceptionTranslationMiddlewareUnaryOutput(GRPCClientMiddleware):
    """Translate client exception"""

    def __init__(self, client_label, server_label, code_to_exception_class_func):
        """Initialize"""
        super(ClientExceptionTranslationMiddlewareUnaryOutput, self).__init__(
            client_label, server_label, GRPCClientUnaryOutputInterceptor
        )
        self._code_to_exception_class_func = code_to_exception_class_func

    class Translator(object):
        """Decorator that wraps a function in a exception translator"""

        def __init__(self, code_to_exception_class_func):
            """Initializes with the counter object"""
            self._code_to_exception_class_func = code_to_exception_class_func

        def __call__(self, fn):
            """Wrap a method with an exception counter"""

            @functools.wraps(fn)
            def wrap(request, context):
                """Execute a function, if an exception is raised, change its type if necessary"""
                try:
                    result = fn(request, context)
                    if result.code() is grpc.StatusCode.OK:
                        return result
                    else:
                        raise result
                except grpc.RpcError as exc:
                    raise_exception_from_grpc_exception(self._code_to_exception_class_func, exc)

            return wrap

    def get_decorator(self, method_name, _):
        """Return exception translator decorator"""
        return self.Translator(self._code_to_exception_class_func)


class ClientRetryingMiddlewareUnaryOutput(GRPCClientMiddleware):
    """Translate client exception"""

    def __init__(self, client_label, server_label, exceptions_to_retry, max_retries):
        """Initialize"""
        super(ClientRetryingMiddlewareUnaryOutput, self).__init__(
            client_label, server_label, GRPCClientUnaryOutputInterceptor
        )
        self._exceptions_to_retry = exceptions_to_retry
        self._max_retries = max_retries

    class Retrier(object):
        """Decorator that wraps a function in a exception translator"""

        def __init__(self, exceptions_to_retry, max_retries):
            """Initializes with the counter object"""
            self._exceptions_to_retry = exceptions_to_retry
            self._max_retries = max_retries

        def __call__(self, fn):
            """Wrap a method with an exception counter"""
            return backoff.on_exception(backoff.expo, self._exceptions_to_retry, self._max_retries)(
                fn
            )

    def get_decorator(self, method_name, _):
        """Return exception translator decorator"""
        return self.Retrier(self._exceptions_to_retry, self._max_retries)


def raise_exception_from_grpc_exception(code_to_exception_class_func, exc):
    """Raise exception from exc, translating with code_to_exception_class_func"""
    code = None
    details = "[]"  # Details are expected to be jsondeserializable

    if exc.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
        raise TimeoutError()
    elif exc.code() == grpc.StatusCode.UNIMPLEMENTED:
        raise NotImplementedError()
    elif exc.code() == grpc.StatusCode.UNAVAILABLE:
        raise ConnectionRefusedError()

    for key, value in exc.trailing_metadata():
        if key == "error_code":
            try:
                code = int(value)
            except (TypeError, ValueError):
                pass
        elif key == "error_details":
            details = value

    if code_to_exception_class_func:
        exception_class = code_to_exception_class_func(code)

        if exception_class:
            exception_args = json.loads(details)
            raise exception_class(*exception_args)
    raise exc


def _get_metadata_map_from_client_details(client_call_details):
    """Get metadata key->value map from client_call_details"""
    metadata = {metadatum[0]: metadatum[1] for metadatum in (client_call_details.metadata or [])}
    return metadata
