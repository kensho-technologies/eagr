# Copyright 2020-present Kensho Technologies, LLC.
import unittest
from unittest.mock import call, create_autospec, MagicMock, patch

from eagr.reflection import reflection_descriptor_database, grpc_reflection_interface
from eagr.tests.reflection import utils

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


class TestGRPCReflectionInterface(unittest.TestCase):
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
