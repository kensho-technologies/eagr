# Copyright 2020-present Kensho Technologies, LLC.
from typing import Callable, Dict, Optional, Tuple

import backoff
from google.protobuf import json_format
import grpc

from eagr.grpc_utils import method, response_translation
from eagr.reflection import reflection_descriptor_database


MAX_RETRIES = 3


def _make_json_to_json_method_invocation(
    method_callable: Callable, proto_type: Callable, max_retries: int
) -> Callable:
    """Make function wrapping grpc method into json conversion"""
    # This invocation definition serves as a lambda for a GRPC method invocation
    @backoff.on_exception(backoff.expo, (grpc.RpcError,), max_retries)
    def invocation(
        input_dict: Dict,
        timeout: Optional[int] = None,
        metadata: Optional[Tuple] = None,
        credentials: Optional[grpc.CallCredentials] = None,
        wait_for_ready: Optional[bool] = None,
        compression: Optional[int] = None,
    ) -> Dict:
        """Convert input into protobuf, invoke grpc and convert response back to json"""
        input_message = proto_type()
        json_format.ParseDict(input_dict, input_message)
        response_as_message = method_callable(
            input_message, timeout=timeout, metadata=metadata, credentials=credentials
        )
        response_as_dict = response_translation.translate_message_to_dict(response_as_message)
        return response_as_dict

    return invocation


def make_json_grpc_client(host: str, service_name: str) -> Dict[str, Callable]:
    """Mount all GRPC methods for service onto client."""
    client = {}  # map method names to method invocations
    channel = grpc.insecure_channel(host, (("grpc.lb_policy_name", "round_robin"),))
    build_database_from_channel = reflection_descriptor_database.build_database_from_channel
    descriptor_pool_instance, symbol_database_instance = build_database_from_channel(channel)

    service_descriptor = descriptor_pool_instance.FindServiceByName(service_name)

    for method_descriptor in service_descriptor.methods:
        method_name = method_descriptor.name
        method_callable = method.make_grpc_unary_method(
            channel, service_name, method_descriptor, symbol_database_instance
        )
        input_type = method_descriptor.input_type
        input_prototype = symbol_database_instance.GetPrototype(input_type)
        method_invocation = _make_json_to_json_method_invocation(
            method_callable, input_prototype, max_retries=MAX_RETRIES
        )

        client[method_name] = method_invocation

    return client
