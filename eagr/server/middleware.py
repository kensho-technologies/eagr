# Copyright 2020-present Kensho Technologies, LLC.
import cProfile
import functools
import json
import logging

from google.protobuf import json_format
from google.protobuf.message import Message as ProtoMessage
import grpc
from grpc import ServerInterceptor
import prometheus_client


logger = logging.getLogger(__name__)


GRPC_ENDPOINT_METRIC_NAME = "grpc_endpoint"
SERVICE_LABEL = "service"
ENDPOINT_LABEL = "endpoint"
ENDPOINT_METRIC_LABELS = (SERVICE_LABEL, ENDPOINT_LABEL)

METRICS_HISTO = prometheus_client.Histogram(
    GRPC_ENDPOINT_METRIC_NAME,
    "Response time histogram for grpc endpoints",
    labelnames=ENDPOINT_METRIC_LABELS,
)


def _wrap_rpc_handler(method_handler, wrapper):
    """Wrap a GRPC rpc handler object in a decorator

    GRPC rpc handlers carry around metadata in addition to the underlying
    method.  _wrap_rpc_handler takes care of the serialization metadata and
    underlying rpc type, just adding a decorator to the method.

    Args:
        method_handler: _InterceptorRpcMethodHandler
        wrapper: callable

    Returns:
        an rpc_method_handler
    """
    if method_handler.request_streaming:
        if method_handler.response_streaming:
            factory = grpc.stream_stream_rpc_method_handler
            fn = method_handler.stream_stream
        else:
            factory = grpc.stream_unary_rpc_method_handler
            fn = method_handler.stream_unary
    else:
        if method_handler.response_streaming:
            factory = grpc.unary_stream_rpc_method_handler
            fn = method_handler.unary_stream
        else:
            factory = grpc.unary_unary_rpc_method_handler
            fn = method_handler.unary_unary
    return factory(
        behavior=wrapper(fn),
        request_deserializer=method_handler.request_deserializer,
        response_serializer=method_handler.response_serializer,
    )


def _service_and_endpoint_labels_from_method(method_name):
    """Get normalized service_label, endpoint_label tuple from method name"""
    name_parts = method_name.split("/")
    if len(name_parts) != 3 or name_parts[0] != "" or name_parts[1] == "" or name_parts[2] == "":
        raise AssertionError("Invalid method name: {}".format(method_name))

    service_label = name_parts[1].replace(".", "_")
    endpoint_label = name_parts[2].replace(".", "_")
    return service_label, endpoint_label


class GRPCMiddleware(object):
    """Base class for GRPC middleware.

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

    def get_interceptors(self):
        """Get a list of interceptors needed by the middleware."""
        return [self.MiddlewareInterceptor(self.get_decorator)]

    class MiddlewareInterceptor(ServerInterceptor):
        """Default GRPC interceptor used by middleware.  Applies a decorator"""

        def __init__(self, decorator_fn):
            """Initialize interceptor with a factory function producing decorators"""
            super(GRPCMiddleware.MiddlewareInterceptor, self).__init__()
            self._decorator_fn = decorator_fn

        def intercept_service(self, continuation, handler_call_details):
            """Interceptor implementation"""
            handler = continuation(handler_call_details)
            metadata = {
                metadatum.key: metadatum.value
                for metadatum in handler_call_details.invocation_metadata
            }
            decorator = self._decorator_fn(handler_call_details.method, metadata)
            # Note that handler may be None in which case we can't apply the
            # decorator and just propagate None
            if decorator and handler:
                handler = _wrap_rpc_handler(handler, decorator)
            return handler


class ProfilerMiddleware(GRPCMiddleware):
    """GRPC middleware that optionally profiles an RPC method"""

    class Profiler(object):
        """Profiling decorator"""

        def __init__(self, profile_mode):
            """Capture the profile mode (tottime or cumtime)"""
            self._profile_mode = profile_mode

        def __call__(self, fn):
            """Profile the rpc and print the resulting stats"""

            @functools.wraps(fn)
            def wrap(request, context):
                """Inner wrapper"""
                profile = cProfile.Profile()
                response = profile.runcall(fn, request, context)
                profile.print_stats(sort=self._profile_mode)
                return response

            return wrap

    def get_decorator(self, _, metadata):
        """If the client requests a profile return a decorator that profiles the RPC"""
        profile_mode = metadata.get("profile")
        if profile_mode and profile_mode in ("tottime", "cumtime"):
            logger.info("Profiling function invocation")
            return self.Profiler(profile_mode)
        elif profile_mode:
            logger.warning("Unknown profile mode {}. Skipping".format(profile_mode))
        return None


class MetricsMiddleware(GRPCMiddleware):
    """GRPC middleware that captures prometheus metrics"""

    def __init__(self):
        """Initialize"""
        super(MetricsMiddleware, self).__init__()

    class Timer(object):
        """Decorator that wraps a function in a prometheus histogram"""

        def __init__(self, histogram):
            """Initializes with the histogram object"""
            self._histogram = histogram

        def __call__(self, fn):
            """Wrap a method with a histogram"""

            @functools.wraps(fn)
            def wrap(request, context):
                """Inner wrapper"""
                with self._histogram.time():
                    return fn(request, context)

            return wrap

    def get_decorator(self, method_name, _):
        """Normalize metric name and return decorator that captures metrics"""
        # Make sure that the method name is valid
        service_label, endpoint_label = _service_and_endpoint_labels_from_method(method_name)
        return self.Timer(
            METRICS_HISTO.labels(**{SERVICE_LABEL: service_label, ENDPOINT_LABEL: endpoint_label})
        )


class ErrorMetaMiddleware(GRPCMiddleware):
    """GRPC middleware that translates exceptions into GRPC codes"""

    def __init__(self, exception_class_to_code_func):
        """Initialize middleware with a function that translates exceptions to codes"""
        self._exception_class_to_code_func = exception_class_to_code_func

    class ExceptionMapper(object):
        """Decorator that translates exceptions"""

        def __init__(self, mapper_func):
            """Capture mapper function"""
            self._mapper_func = mapper_func

        def __call__(self, fn):
            """Wrap a method with the exception translator"""

            @functools.wraps(fn)
            def wrap(request, context):
                """Inner wrapper"""
                try:
                    return fn(request, context)
                except Exception as e:
                    code = self._mapper_func(type(e))
                    details = json.dumps(list(e.args))
                    # If we have the code, use that. Otherwise we have to live with the default
                    if code:
                        context.set_trailing_metadata(
                            (("error_code", str(code)), ("error_details", details))
                        )
                        # Note that at this point GRPC will reset the code, but we can always hope
                        context.set_code(code)
                        context.set_details(details)
                    raise

            return wrap

    def get_decorator(self, _, __):
        """Return exception mapping decorator"""
        return self.ExceptionMapper(self._exception_class_to_code_func)


class LoggingMiddleware(GRPCMiddleware):
    """GRPC middleware that captures invocation logs."""

    def __init__(self, sanitizer=None):
        """Initialize"""
        super(LoggingMiddleware, self).__init__()
        self._sanitizer = sanitizer

    class Logger(object):
        """Decorator that wraps a function in a prometheus histogram"""

        def __init__(self, service, method, sanitizer):
            """Initializes with the service and method names"""
            self._service = service
            self._method = method
            self._sanitizer = sanitizer

        def __call__(self, fn):
            """Wrap a method with a histogram"""

            @functools.wraps(fn)
            def wrap(request, context):
                """Inner wrapper"""
                if isinstance(request, ProtoMessage):
                    sanitized_request = self._sanitizer(request) if self._sanitizer else request
                    logger.info(
                        "Invoked %s.%s(%s)",
                        self._service,
                        self._method,
                        str(json_format.MessageToDict(sanitized_request)),
                    )
                else:
                    logger.info(
                        "Invoked %s.%s with non-protobuf parameter", self._service, self._method
                    )

                return fn(request, context)

            return wrap

    def get_decorator(self, method_name, _):
        """Normalize metric name and return decorator that captures metrics"""
        # Make sure that the method name is valid
        service_label, endpoint_label = _service_and_endpoint_labels_from_method(method_name)
        return self.Logger(service_label, endpoint_label, self._sanitizer)
