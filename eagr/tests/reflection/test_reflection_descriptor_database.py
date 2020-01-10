# Copyright 2020-present Kensho Technologies, LLC.
import unittest
from unittest.mock import call, create_autospec

from google.protobuf import descriptor_pb2
from google.protobuf.descriptor_pool import DescriptorPool
from grpc_reflection.v1alpha import reflection_pb2

from eagr.reflection import reflection_descriptor_database
from eagr.tests.reflection import utils


class TestReflectionDescriptorDatabase(unittest.TestCase):
    def test_get_proto_bytes_for_requests(self):
        # Test empty case
        file_requests_empty = []
        proto_bytes_empty = reflection_descriptor_database.get_proto_bytes_for_requests(
            utils.reflection_client_mock, file_requests_empty
        )
        self.assertEqual([], proto_bytes_empty)

        # Test non-empty cases
        file_requests = (
            reflection_pb2.ServerReflectionRequest(file_by_filename=request)
            for request in ["proto_a", "proto_c"]
        )
        proto_bytes = reflection_descriptor_database.get_proto_bytes_for_requests(
            utils.reflection_client_mock, file_requests
        )
        self.assertIn(utils.PROTO_BYTES_A, proto_bytes)
        self.assertIn(utils.PROTO_BYTES_C, proto_bytes)
        self.assertNotIn(utils.PROTO_BYTES_B, proto_bytes)

    def test_import_dependencies_then_proto(self):
        descriptor_pool_mock = create_autospec(spec=DescriptorPool(), spec_set=True)
        # Test detached proto_a
        parsed_proto_bytes_a = descriptor_pb2.FileDescriptorProto()
        parsed_proto_bytes_a.ParseFromString(utils.PROTO_BYTES_A)
        reflection_descriptor_database.import_dependencies_then_proto(
            utils.reflection_client_mock,
            descriptor_pool_mock,
            "proto_a",
            set(),
            {"proto_a": parsed_proto_bytes_a},
        )
        expected_calls = [call(utils.PROTO_A)]
        descriptor_pool_mock.Add.assert_has_calls(expected_calls, any_order=False)

        # Test proto_b including dependencies in a tree
        descriptor_pool_mock = create_autospec(spec=DescriptorPool(), spec_set=True)
        parsed_proto_bytes_b = descriptor_pb2.FileDescriptorProto()
        parsed_proto_bytes_b.ParseFromString(utils.PROTO_BYTES_B)
        reflection_descriptor_database.import_dependencies_then_proto(
            utils.reflection_client_mock,
            descriptor_pool_mock,
            "proto_b",
            set(),
            {"proto_b": parsed_proto_bytes_b},
        )

        expected_calls = [
            call(utils.PROTO_C),
            call(utils.PROTO_E),
            call(utils.PROTO_F),
            call(utils.PROTO_D),
            call(utils.PROTO_B),
        ]
        descriptor_pool_mock.Add.assert_has_calls(expected_calls, any_order=False)

    def test_add_protos_to_descriptor_pool(self):
        descriptor_pool_mock = create_autospec(spec=DescriptorPool(), spec_set=True)

        # Test empty case
        reflection_descriptor_database.add_protos_to_descriptor_pool(
            utils.reflection_client_mock, descriptor_pool_mock, []
        )
        self.assertEqual(len(descriptor_pool_mock.Add.call_args_list), 0)

        # Test non-empty case
        reflection_descriptor_database.add_protos_to_descriptor_pool(
            utils.reflection_client_mock,
            descriptor_pool_mock,
            [utils.PROTO_BYTES_A, utils.PROTO_BYTES_B],
        )
        expected_calls = [
            call(utils.PROTO_A),
            call(utils.PROTO_C),
            call(utils.PROTO_E),
            call(utils.PROTO_F),
            call(utils.PROTO_D),
            call(utils.PROTO_B),
        ]
        descriptor_pool_mock.Add.assert_has_calls(expected_calls, any_order=False)

    def test_build_database_from_stub(self):
        # Only need to test to make sure it runs without error
        reflection_descriptor_database.build_database_from_stub(utils.reflection_client_mock)
