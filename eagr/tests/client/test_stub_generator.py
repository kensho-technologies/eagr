# Copyright 2020-present Kensho Technologies, LLC.
"""Testing various wrappers that make_grpc_client creates"""
import time
import unittest
from unittest.mock import MagicMock

from google.protobuf.wrappers_pb2 import StringValue
from opentracing import global_tracer, set_global_tracer
from prometheus_client.core import REGISTRY

from ...client import make_grpc_client
from ...client.client_test_helpers import inprocess_grpc_server
from ...protos import test_service_pb2_grpc


def _get_metric_value(metrics, metric_name, sample_name, labels):
    """Get value for sample of a metric"""
    for metric in metrics:
        if metric.name == metric_name:
            for sample in metric.samples:
                if sample.name == sample_name and sample.labels == labels:
                    return sample.value
    return None


class TestCallMetrics(unittest.TestCase):
    def test_call_metrics(self):
        servicer = MagicMock(test_service_pb2_grpc.TestServiceServicer)
        servicer.UnaryUnary = lambda x, _: x
        with inprocess_grpc_server(
            servicer, test_service_pb2_grpc.add_TestServiceServicer_to_server
        ) as address:
            set_global_tracer(global_tracer())
            client = make_grpc_client("foo", "bar", address, test_service_pb2_grpc.TestServiceStub)
            unary_value = StringValue(value="foo")
            self.assertEqual(unary_value, client.UnaryUnary(unary_value))
            labels = {
                "client_name": "foo",
                "server_name": "bar",
                "service": "eagr_TestService",
                "endpoint": "UnaryUnary",
            }
            call_count = REGISTRY.get_sample_value("clientside_grpc_endpoint_count", labels=labels)
            self.assertEqual(1, call_count)
            # Try that again to see if the number changes
            self.assertEqual(unary_value, client.UnaryUnary(unary_value))
            call_count = REGISTRY.get_sample_value("clientside_grpc_endpoint_count", labels=labels)
            self.assertEqual(2, call_count)

    def test_exception_tracking(self):
        servicer = MagicMock(test_service_pb2_grpc.TestServiceServicer)
        servicer.UnaryUnary = lambda x, _: time.sleep(10)
        count_labels = {
            "client_name": "foo",
            "server_name": "bar",
            "service": "eagr_TestService",
            "endpoint": "UnaryUnary",
        }
        exception_labels = dict(count_labels, exception="TimeoutError")

        with inprocess_grpc_server(
            servicer, test_service_pb2_grpc.add_TestServiceServicer_to_server
        ) as address:
            set_global_tracer(global_tracer())
            client = make_grpc_client("foo", "bar", address, test_service_pb2_grpc.TestServiceStub)
            calls_before = (
                REGISTRY.get_sample_value("clientside_grpc_endpoint_count", labels=count_labels)
                or 0
            )
            exceptions_before = (
                REGISTRY.get_sample_value(
                    "clientside_grpc_endpoint_error_total", labels=exception_labels
                )
                or 0
            )
            with self.assertRaises(TimeoutError):
                client.UnaryUnary(StringValue(value="foo"), timeout=1)
            calls_after = REGISTRY.get_sample_value(
                "clientside_grpc_endpoint_count", labels=count_labels
            )
            exceptions_after = REGISTRY.get_sample_value(
                "clientside_grpc_endpoint_error_total", labels=exception_labels
            )

            self.assertEqual(1, calls_after - calls_before)
            print(exceptions_after, exceptions_before)  # noqa
            self.assertEqual(1, exceptions_after - exceptions_before)
