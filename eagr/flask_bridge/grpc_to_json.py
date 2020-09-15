# Copyright 2020-present Kensho Technologies, LLC.
"""Generic code that allows mounting grpc service calls on "REST" passthroughs"""

import flask
from google.protobuf import descriptor_pool, json_format, symbol_database

# Fetch a selected set of primitive types for which the protodict clean up involves
# a flattening of the namespace.
from eagr.grpc_utils.method import make_grpc_unary_method
from eagr.grpc_utils.response_translation import translate_message_to_dict
from eagr.reflection.reflection_descriptor_database import build_database_from_channel


def dict_to_grpc_method_handler(method, input_type, input_dict, symbol_database_instance):
    # type (Callable, MessageDescriptor, Dict[str, Any], Any) -> Dict
    """Invoke method identified by method_name of stub by converting dict payload into protobuf.

    Can handle only methods that accept a single protobuf as an input and produce a single
    protobuf (not stream) as an output

    Args:
        method: method to invoke
        input_type: message descriptor for the input proto
        input_dict: dictionary containing input data (that can be converted into proto using
            json_format.ParseDict)
        symbol_database_instance: symbol_database_instance

    Returns:
        dict containing the response
    """
    # Note that this is smart enough to process even well-known types e.g. StringValue
    input_prototype = symbol_database_instance.GetPrototype(input_type)
    input_message = input_prototype()
    json_format.ParseDict(input_dict, input_message)
    response_as_message = method(input_message)
    response_as_dict = translate_message_to_dict(response_as_message)
    return response_as_dict


def make_flask_dict_handler(method, func_name, input_type, symbol_database_instance):
    # type (Channel, str, MessageDescriptor, Any) -> Callable
    """Make a flask handler for the method.

    Args:
        method: method
        func_name: function name.
        input_type: type of input
        symbol_database_instance: symbol db instance
    """

    def method_handler():
        """Method handler."""
        input_dict = flask.request.get_json()
        response = dict_to_grpc_method_handler(
            method, input_type, input_dict, symbol_database_instance
        )
        return flask.json.jsonify(response)

    method_handler.__name__ = func_name
    return method_handler


def map_and_mount(
    flask_app,
    channel,
    service_name,
    json_service_path,
    descriptor_pool_instance=None,
    symbol_database_instance=None,
):
    # type (flask.Flask, Channel, str, str, Any, Any) -> None
    """Mount all json passthrough methods on specific path.

    Args:
        flask_app: flask app
        channel: grpc channel
        service_name: service name
        json_service_path: jsons service path
        descriptor_pool_instance: descriptor pool instance
        symbol_database_instance: symbol db instance
    """
    if descriptor_pool_instance is None:
        descriptor_pool_instance = descriptor_pool.Default()
    if symbol_database_instance is None:
        symbol_database_instance = symbol_database.Default()
    service_descriptor = descriptor_pool_instance.FindServiceByName(service_name)
    for method_descriptor in service_descriptor.methods:
        method_name = method_descriptor.name
        method_callable = make_grpc_unary_method(
            channel, service_name, method_descriptor, symbol_database_instance
        )
        input_type = method_descriptor.input_type
        method_handler = make_flask_dict_handler(
            method_callable, method_name, input_type, symbol_database_instance
        )
        method_route = "{}/{}".format(json_service_path, method_name)
        flask_app.route(method_route, methods=["POST"])(method_handler)


def map_and_mount_remote_server(flask_app, channel, service_name, json_service_path):
    # type (flask.Flask, Channel, str, str) -> None
    """Mount a remote service onto the flask app.

    Args:
        flask_app: flask_app
        channel: grpc channel
        service_name: service name
        json_service_path: json_service_path
    """
    descriptor_pool_instance, symbol_database_instance = build_database_from_channel(channel)
    return map_and_mount(
        flask_app,
        channel,
        service_name,
        json_service_path,
        descriptor_pool_instance=descriptor_pool_instance,
        symbol_database_instance=symbol_database_instance,
    )
