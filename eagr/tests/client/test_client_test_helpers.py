# Copyright 2020-present Kensho Technologies, LLC.
import unittest

from google.protobuf.wrappers_pb2 import StringValue
import grpc

from ...client.client_test_helpers import inprocess_grpc_server
from ...protos import test_service_pb2_grpc


class Servicer(test_service_pb2_grpc.TestServiceServicer):
    """Stupid Servicer"""

    def UnaryUnary(self, req, context):
        """Reflection"""
        return req

    def UnaryStream(self, req, context):
        """Reflect 10 times"""
        for _ in range(10):
            yield req

    def StreamUnary(self, req, context):
        """Concatenation"""
        response = ""
        for x in req:
            response += x.value
        return StringValue(value=response)

    def StreamStream(self, req, context):
        """Reflection"""
        for x in req:
            yield x


class TestClientTestHelpers(unittest.TestCase):
    def test_methods(self):
        with inprocess_grpc_server(
            Servicer(), test_service_pb2_grpc.add_TestServiceServicer_to_server
        ) as address:
            channel = grpc.insecure_channel(address)
            stub = test_service_pb2_grpc.TestServiceStub(channel)

            unary_value = StringValue(value="foo")
            self.assertEqual(unary_value, stub.UnaryUnary(unary_value))
            self.assertEqual([unary_value] * 10, list(stub.UnaryStream(unary_value)))

            data_to_stream = [
                StringValue(value="foo"),
                StringValue(value="bar"),
                StringValue(value="baz"),
            ]

            self.assertEqual(
                StringValue(value="foobarbaz"), stub.StreamUnary((x for x in data_to_stream))
            )
            self.assertEqual(data_to_stream, list(stub.StreamStream((x for x in data_to_stream))))
