# Copyright 2020-present Kensho Technologies, LLC.
import unittest

import grpc

from ...client.client_wrapper import (
    _with_generator_error_translation,
    _with_unary_function_error_translation,
    raise_exception_from_grpc_exception,
)


class FakeRpcError(grpc.RpcError):
    def __init__(self, code, details):
        """Create a fake rpc error with given code and details"""
        self._trailing_metadata = {}
        self._trailing_metadata["error_code"] = code
        self._trailing_metadata["error_details"] = details

    def trailing_metadata(self):
        """Return trailing metadata for the exception"""
        return self._trailing_metadata.items()


class ExceptionOne(Exception):
    """A dummy exception"""


def code_to_class(code):
    """Fake code to class"""
    if code == -1:
        return ExceptionOne

    return None


class ClientWrapperTests(unittest.TestCase):
    def test_raise_exception_from_grpc_exception(self):
        """Test that exceptions get converted properly"""

        # Verify that gRPC error that has no translation table, gets raised as a grpc exception
        non_registered_exc = FakeRpcError(-2, '["some details"]')  # Non-registered exception

        with self.assertRaises(grpc.RpcError):
            raise_exception_from_grpc_exception(code_to_class, non_registered_exc)

        exc = FakeRpcError(-1, '["some details"]')
        try:
            raise_exception_from_grpc_exception(code_to_class, exc)
        except ExceptionOne as e:
            self.assertIn("some details", str(e))

    def test_with_unary_function_error_translation(self):
        """Test that client errors get translated correctly"""

        def fail():
            """Raise an error invoking"""
            raise FakeRpcError(-1, '["some more details"]')

        decorated_fail = _with_unary_function_error_translation(code_to_class, fail)

        try:
            decorated_fail()
        except ExceptionOne as e:
            self.assertIn("some more details", str(e))

    def test_generator_wrapper(self):
        """Test wrapping a generator"""

        def fail_yielding():
            """Yield one item then raise an error"""
            yield 0
            raise FakeRpcError(-1, '["failed yielding"]')

        decorated_fail = _with_generator_error_translation(code_to_class, fail_yielding)
        gen = decorated_fail()

        self.assertEqual(next(gen), 0)

        try:
            next(gen)
        except ExceptionOne as e:
            self.assertIn("failed yielding", str(e))
