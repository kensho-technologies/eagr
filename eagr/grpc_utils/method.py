# Copyright 2020-present Kensho Technologies, LLC.
def make_grpc_unary_method(channel, service_name, method_descriptor, symbol_database_instance):
    # type (Channel, str, MethodDescriptor, Any) -> Callable
    """Make grp callable on the channel.

    Args:
        channel: grpc channel
        service_name: name of service
        method_descriptor: method descriptor
        symbol_database_instance: symbol db instance
    """
    input_prototype = symbol_database_instance.GetPrototype(method_descriptor.input_type)
    output_prototype = symbol_database_instance.GetPrototype(method_descriptor.output_type)
    method = channel.unary_unary(
        "/{}/{}".format(service_name, method_descriptor.name),
        request_serializer=input_prototype.SerializeToString,
        response_deserializer=output_prototype.FromString,
    )
    return method
