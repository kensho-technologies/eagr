# Copyright 2020-present Kensho Technologies, LLC.
# Functionality to propagate exception information through the grpc
import json

import funcy
import grpc


def _is_grpc_unary_method(attr):
    """Check if attribute is a grpc method that returns unary."""
    return isinstance(attr, (grpc.UnaryUnaryMultiCallable, grpc.StreamUnaryMultiCallable))


def _is_grpc_stream_method(attr):
    """Check if attribute is a grpc method that returns a stream."""
    return isinstance(attr, (grpc.UnaryStreamMultiCallable, grpc.StreamStreamMultiCallable))


def _with_unary_function_error_translation(code_to_exception_class_func, func):
    """A decorator that translates server-side exceptions into clientside ones.

    Clientside jsonrpclib translates all unknown errors into ProtocolErrors initialized with
    a tuple (code, message). We assume that the errors have been encoded by flask_jsonrpc via
    with_server_error_translation and we can use code_to_exception_class_func to go from the code
    to an exception class that can be constructed with args deserialized from the message. If the
    exception cannot be retrieved, then we re-raise the protocol error.

    Args:
        code_to_exception_class_func: function that returns an exception corresponding to the code
                                      or None
        func: function to be returned

    Returns:
        wrapped function that extracts the server-side exceptions from those raised by jsonrpclib
    """

    @funcy.wraps(func)
    def decorated(*args, **kwargs):
        """Execute a function, if an exception is raised, change its type if necessary"""
        try:
            return func(*args, **kwargs)
        except grpc.RpcError as exc:
            raise_exception_from_grpc_exception(code_to_exception_class_func, exc)

    return decorated


def _with_generator_error_translation(code_to_exception_class_func, func):
    """Same wrapping as above, but for a generator"""

    @funcy.wraps(func)
    def decorated(*args, **kwargs):
        """Execute a function, if an exception is raised, change its type if necessary"""
        try:
            for x in func(*args, **kwargs):
                yield x
        except grpc.RpcError as exc:
            raise_exception_from_grpc_exception(code_to_exception_class_func, exc)

    return decorated


def raise_exception_from_grpc_exception(code_to_exception_class_func, exc):
    """Raise exception from exc, translating with code_to_exception_class_func"""
    code = None
    details = "[]"  # Details are expected to be jsondeserializable

    for key, value in exc.trailing_metadata():
        if key == "error_code":
            try:
                code = int(value)
            except (TypeError, ValueError):
                pass
        elif key == "error_details":
            details = value

    exception_class = code_to_exception_class_func(code)

    if exception_class:
        exception_args = json.loads(details)
        raise exception_class(*exception_args)
    else:
        raise exc


def update_grpc_stub_with_wrappers(stub, code_to_exception_class_func=None):
    """Update the stub's GRPC methods by applying decorators"""
    for attr_name in dir(stub):
        attr = getattr(stub, attr_name)

        if code_to_exception_class_func:
            if _is_grpc_unary_method(attr):
                setattr(
                    stub,
                    attr_name,
                    _with_unary_function_error_translation(code_to_exception_class_func, attr),
                )
            elif _is_grpc_stream_method(attr):
                setattr(
                    stub,
                    attr_name,
                    _with_generator_error_translation(code_to_exception_class_func, attr),
                )
            else:
                pass

    return stub
