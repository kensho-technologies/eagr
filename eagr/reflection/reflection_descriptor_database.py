# Copyright 2020-present Kensho Technologies, LLC.
"""Code for loading descriptor and symbol databases from grpc reflection service

This is Leonid's personal take on clean room implementation of
https://github.com/grpc/grpc/blob/master/test/cpp/util/proto_reflection_descriptor_database.cc
but with less bells and whistles.

In particular, the descriptor pool is filled in in a single shot, we do not hold client to
incrementally load additional data. Given that the only things we care about is the information
about all services on the server, we first ask for all the services, and then load data for each for
them using file_containing_symbol request.

The fear is that this might not be able to handle imports (i.e. what if the service depends on the
messages defined in other proto files). But this should not be a problem according to documentation
https://github.com/grpc/grpc/blob/master/doc/server-reflection.md in particular:
```
Because most usecases will require also requesting the transitive dependencies of requested files,
the queries will also return all transitive dependencies of the returned file.
Should interesting usecases for non-transitive queries turn up later, we can easily extend the
protocol to support them.
```
note that the response for file_containing_symbol request contains a list of descriptors, not a
single one.
"""

import funcy
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.symbol_database import SymbolDatabase
from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc


GET_SERVICES_REQUEST = reflection_pb2.ServerReflectionRequest(list_services="services")


def get_proto_bytes_for_requests(reflection_client, file_requests):
    """Return the file descriptor proto bytes list given file requests for services or files

    Args:
        reflection_client: ServerReflectionStub. GRPC reflection client
        file_requests: generator (ServerReflectionRequest). The requests for file descriptor
            protos to be queried on during server reflection

    Returns:
        bytes
    """
    file_descriptors_responses = reflection_client.ServerReflectionInfo(file_requests)
    proto_bytes = [
        file_descriptor_proto
        for response in file_descriptors_responses
        for file_descriptor_proto in response.file_descriptor_response.file_descriptor_proto
    ]
    return proto_bytes


def import_dependencies_then_proto(
    reflection_client, descriptor_pool, proto_name, imported_names, reflected_names
):
    """Recursively add file descriptor protos to descriptor pool, first importing dependencies

    Args:
        reflection_client: ServerReflectionStub. GRPC reflection client
        descriptor_pool: DescriptorPool. GRPC descriptor pool where file descriptor protos will be
            loaded
        proto_name: string. Name of the file desciptor proto to be imported
        imported_names: set. The names of the memoized, already imported file descriptor protos
        reflected_names: dict. Map of to-be-imported file descriptor proto names to their
            respective file descriptor protos

    Returns:
        None
    """
    if proto_name in imported_names:
        return

    # Anything in the proto_name depth-first search stack should already be reflected
    if proto_name not in reflected_names:
        raise RuntimeError(
            "Something went wrong. Stacked planned imports should either "
            "be already reflected or already imported. Please fix."
        )

    dependencies = reflected_names[proto_name].dependency  # pylint: disable=no-member
    # Reflect not-yet-reflected dependencies the same way we run server reflection on root protos
    not_yet_reflected_dependencies = []
    for dependency in dependencies:
        if (dependency not in imported_names) and (dependency not in reflected_names):
            not_yet_reflected_dependencies.append(dependency)

    if not_yet_reflected_dependencies:
        file_requests = (
            reflection_pb2.ServerReflectionRequest(file_by_filename=dependency)
            for dependency in not_yet_reflected_dependencies
        )
        file_descriptor_proto_bytes = get_proto_bytes_for_requests(reflection_client, file_requests)
        for serialized_proto in file_descriptor_proto_bytes:
            parsed_file_descriptor_proto = descriptor_pb2.FileDescriptorProto()
            parsed_file_descriptor_proto.ParseFromString(serialized_proto)
            file_descriptor_proto_name = (
                parsed_file_descriptor_proto.name  # pylint: disable=no-member
            )
            reflected_names[file_descriptor_proto_name] = parsed_file_descriptor_proto

    # Recursively import dependencies
    for dependency in dependencies:
        import_dependencies_then_proto(
            reflection_client, descriptor_pool, dependency, imported_names, reflected_names
        )

    # Import the proto itself, update memos
    descriptor_pool.Add(reflected_names[proto_name])
    imported_names.add(proto_name)
    del reflected_names[proto_name]


def add_protos_to_descriptor_pool(reflection_client, descriptor_pool, file_descriptor_proto_bytes):
    """Add protos and dependencies, bottom-up, to the protobuf descriptor pool

    Args:
        reflection_client: ServerReflectionStub. GRPC reflection client
        descriptor_pool: DescriptorPool. GRPC descriptor pool where file descriptor protos will be
            loaded
        file_descriptor_proto_bytes: list of proto_bytes strings for the root GRPC file descriptor
            proto files to be added to the descriptor pool

    Returns:
        None
    """
    # Set of already imported names
    imported_names = set()
    # Map of file descriptor proto names to the respective file descriptor protos
    reflected_names = {}
    # Stack of file descriptor proto names to be imported
    proto_names = []

    # Load the initial proto name stack and reflected names hash with root-level protos to import
    for serialized_proto in file_descriptor_proto_bytes:
        parsed_file_descriptor_proto = descriptor_pb2.FileDescriptorProto()
        parsed_file_descriptor_proto.ParseFromString(serialized_proto)
        file_descriptor_proto_name = parsed_file_descriptor_proto.name  # pylint: disable=no-member
        proto_names.append(file_descriptor_proto_name)
        reflected_names[file_descriptor_proto_name] = parsed_file_descriptor_proto

    # Recursively find file descriptor proto dependencies, and import the entire tree bottom-up
    for proto_name in proto_names:
        import_dependencies_then_proto(
            reflection_client, descriptor_pool, proto_name, imported_names, reflected_names
        )


def build_database_from_stub(reflection_client):
    """Build descriptor pool and symbol database from reflection service.

    Args:
        reflection_client: ServerReflectionStub. GRPC reflection client

    Returns:
        tuple (descriptor pool, symbol database)
    """
    services_response = funcy.first(
        # Note that this is stupid, but grpc has problems iterating over lists
        reflection_client.ServerReflectionInfo(x for x in [GET_SERVICES_REQUEST])
    )
    service_names = [service.name for service in services_response.list_services_response.service]
    file_requests = (
        reflection_pb2.ServerReflectionRequest(file_containing_symbol=service_name)
        for service_name in service_names
    )
    file_descriptor_proto_bytes = get_proto_bytes_for_requests(reflection_client, file_requests)

    descriptor_pool = DescriptorPool()
    add_protos_to_descriptor_pool(reflection_client, descriptor_pool, file_descriptor_proto_bytes)

    symbol_database = SymbolDatabase(descriptor_pool)
    return (descriptor_pool, symbol_database)


def build_database_from_channel(channel):
    """Build descriptor pool and symbol database from reflection service.

    Args:
        channel: GRPC channel

    Returns:
        tuple (descriptor pool, symbol database)
    """
    reflection_client = reflection_pb2_grpc.ServerReflectionStub(channel)
    return build_database_from_stub(reflection_client)
