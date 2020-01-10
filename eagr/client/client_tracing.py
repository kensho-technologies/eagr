# Copyright 2020-present Kensho Technologies, LLC.
from grpc_opentracing import ActiveSpanSource, open_tracing_client_interceptor
from grpc_opentracing.grpcext import intercept_channel
import opentracing
from opentracing_instrumentation.request_context import get_current_span


class RequestContextSpanSource(ActiveSpanSource):
    """Implements interface of getting current span

    Gets span from RequestContext of opentracing instrumentation
    for the grpc opentracing library.
    """

    def get_active_span(self):
        """Get the request context active span"""
        return get_current_span()


def wrap_grpc_client_channel(channel):
    """Wraps a GRPC channel with tracing, given a global tracer has been registered"""
    if not opentracing.is_global_tracer_registered():
        raise Exception(
            "Global tracer has not been registered. Disable tracing or " "register a global tracer"
        )

    interceptor = open_tracing_client_interceptor(
        opentracing.global_tracer(), active_span_source=RequestContextSpanSource()
    )
    return intercept_channel(channel, interceptor)
