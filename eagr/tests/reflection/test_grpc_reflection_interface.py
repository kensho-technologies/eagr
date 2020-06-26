# Copyright 2020-present Kensho Technologies, LLC.
import collections
import unittest
from unittest.mock import MagicMock, patch

from google.protobuf.util import json_format_proto3_pb2

from ...reflection import grpc_reflection_interface, reflection_descriptor_database
from ...tests.reflection import utils


# Mocks to patch out context-specific method calls in eagr.reflection.grpc_reflection_interface
def build_database_from_channel_mock(channel):
    """Returns a built DescriptorPool from a mock ServerReflectionStub, given any channel"""
    return reflection_descriptor_database.build_database_from_stub(utils.reflection_client_mock)


def find_service_by_name_mock(service_name):
    """Returns a mock GRPC service given a valid service name"""
    method_mock = MagicMock()
    method_mock.name = "MyTestMethod"
    method_mock.input_type = MagicMock()
    method_mock.input_type.prototype = build_database_from_channel_mock

    service_mock = MagicMock()
    service_mock.methods = [method_mock]
    if service_name == "good_service":
        return service_mock

    else:
        raise KeyError("Bad service {} given".format(service_name))


def make_grpc_unary_method_mock(channel, service_name, method_descriptor, symbol_database_instance):
    """Returns a mock callable method regardless of params provided"""

    def callable_mock():
        """Return a mock string value invariably"""
        return "return_value"

    return callable_mock


def get_prototype_mock(input_type):
    """Returns a mock callable method regardless of params provided"""

    def callable_mock():
        """Return a mock string value invariably"""
        return "return_value"

    return callable_mock


class ContextMock:
    def __init__(self, metadata):
        metadatum_tuple = collections.namedtuple("_Metadatum", ("key", "value"))
        self._metadata = map(metadatum_tuple._make, metadata)

    def invocation_metadata(self):
        return self._metadata


def method_to_callable_mock(grpc_method):
    def transformed_invocation(
        input_message,
        timeout=None,
        metadata=None,
        credentials=None,
        wait_for_ready=None,
        compression=None,
    ):
        return grpc_method(input_message, ContextMock(metadata))

    return transformed_invocation


class TestGRPCReflectionInterface(unittest.TestCase):
    def test__make_json_to_json_method_invocation(self):
        expected_input = 481516

        # Derived from google.protobuf's test cases
        test_input_dict = {"int32Value": expected_input}
        test_message = json_format_proto3_pb2.TestMessage

        def get_input_and_metadata(request, context):
            metadata_first = next(context.invocation_metadata())
            return test_message(
                string_value="int32_value: {}, metadata {}: {}".format(
                    request.int32_value, metadata_first.key, metadata_first.value
                )
            )

        json_method_invocation = grpc_reflection_interface._make_json_to_json_method_invocation(
            method_to_callable_mock(get_input_and_metadata), test_message, 3
        )

        self.assertEqual(
            json_method_invocation(test_input_dict, metadata=(("hi", "there"),))["stringValue"],
            "int32_value: 481516, metadata hi: there",
        )

    @patch("eagr.reflection.reflection_descriptor_database.SymbolDatabase", autospec=True)
    @patch("eagr.grpc_utils.method.make_grpc_unary_method", autospec=True)
    @patch("eagr.reflection.reflection_descriptor_database.DescriptorPool", autospec=True)
    @patch(
        "eagr.reflection.reflection_descriptor_database.build_database_from_channel", autospec=True
    )
    def test_make_json_grpc_client(
        self,
        build_database_mock_method,
        descriptor_pool_mock,
        make_unary_method_mock_method,
        symbol_database_mock,
    ):
        build_database_mock_method.side_effect = build_database_from_channel_mock

        descriptor_pool_mock_return_value = MagicMock()
        descriptor_pool_mock_return_value.FindServiceByName.side_effect = find_service_by_name_mock
        descriptor_pool_mock.return_value = descriptor_pool_mock_return_value

        make_unary_method_mock_method.side_effect = make_grpc_unary_method_mock

        symbol_database_mock_return_value = MagicMock()
        symbol_database_mock_return_value.GetPrototype.side_effect = get_prototype_mock
        symbol_database_mock.return_value.return_value = symbol_database_mock_return_value

        my_client = grpc_reflection_interface.make_json_grpc_client("my_host", "good_service")

        self.assertIn("MyTestMethod", my_client)

        with self.assertRaises(KeyError) as context:
            my_client = grpc_reflection_interface.make_json_grpc_client("my_host", "service_b")

        self.assertIn("service_b", context.exception.args[0])
