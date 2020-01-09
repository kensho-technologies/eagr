# Copyright 2020-present Kensho Technologies, LLC.
from unittest.mock import MagicMock

from google.protobuf import descriptor_pb2
from grpc_reflection.v1alpha import reflection_pb2_grpc


SERVICE_A = MagicMock()
SERVICE_A.name = "service_a"
SERVICE_B = MagicMock()
SERVICE_B.name = "service_b"
SERVICES_MOCK = MagicMock()
SERVICES_MOCK.list_services_response = MagicMock()
SERVICES_MOCK.list_services_response.service = [SERVICE_A, SERVICE_B]

PROTO_A = descriptor_pb2.FileDescriptorProto(name="proto_a")
PROTO_BYTES_A = PROTO_A.SerializeToString()
PROTO_B = descriptor_pb2.FileDescriptorProto(name="proto_b", dependency=["proto_c", "proto_d"])
PROTO_BYTES_B = PROTO_B.SerializeToString()
PROTO_C = descriptor_pb2.FileDescriptorProto(name="proto_c")
PROTO_BYTES_C = PROTO_C.SerializeToString()
PROTO_D = descriptor_pb2.FileDescriptorProto(name="proto_d", dependency=["proto_e", "proto_f"])
PROTO_BYTES_D = PROTO_D.SerializeToString()
PROTO_E = descriptor_pb2.FileDescriptorProto(name="proto_e")
PROTO_BYTES_E = PROTO_E.SerializeToString()
PROTO_F = descriptor_pb2.FileDescriptorProto(name="proto_f")
PROTO_BYTES_F = PROTO_F.SerializeToString()

RESPONSE_MOCK_A = MagicMock()
RESPONSE_MOCK_A.file_descriptor_response = MagicMock()
RESPONSE_MOCK_A.file_descriptor_response.file_descriptor_proto = [PROTO_BYTES_A]
RESPONSE_MOCK_B = MagicMock()
RESPONSE_MOCK_B.file_descriptor_response = MagicMock()
RESPONSE_MOCK_B.file_descriptor_response.file_descriptor_proto = [PROTO_BYTES_B]
RESPONSE_MOCK_C = MagicMock()
RESPONSE_MOCK_C.file_descriptor_response = MagicMock()
RESPONSE_MOCK_C.file_descriptor_response.file_descriptor_proto = [PROTO_BYTES_C]
RESPONSE_MOCK_D = MagicMock()
RESPONSE_MOCK_D.file_descriptor_response = MagicMock()
RESPONSE_MOCK_D.file_descriptor_response.file_descriptor_proto = [PROTO_BYTES_D]
RESPONSE_MOCK_E = MagicMock()
RESPONSE_MOCK_E.file_descriptor_response = MagicMock()
RESPONSE_MOCK_E.file_descriptor_response.file_descriptor_proto = [PROTO_BYTES_E]
RESPONSE_MOCK_F = MagicMock()
RESPONSE_MOCK_F.file_descriptor_response = MagicMock()
RESPONSE_MOCK_F.file_descriptor_response.file_descriptor_proto = [PROTO_BYTES_F]


def get_file_by_filename(request):
    if request.file_by_filename == "proto_a":
        return RESPONSE_MOCK_A
    elif request.file_by_filename == "proto_b":
        return RESPONSE_MOCK_B
    elif request.file_by_filename == "proto_c":
        return RESPONSE_MOCK_C
    elif request.file_by_filename == "proto_d":
        return RESPONSE_MOCK_D
    elif request.file_by_filename == "proto_e":
        return RESPONSE_MOCK_E
    elif request.file_by_filename == "proto_f":
        return RESPONSE_MOCK_F
    else:
        raise AssertionError("Cannot find file request named {}".format(request.file_by_filename))


def get_file_containing_symbol(request):
    if request.file_containing_symbol == "service_a":
        return RESPONSE_MOCK_A
    if request.file_containing_symbol == "service_b":
        return RESPONSE_MOCK_B
    else:
        raise AssertionError(
            "Cannot find file request with symbol {}".format(request.file_containing_symbol)
        )


def server_reflection_info_mock(reflection_requests):
    responses = []
    for request in reflection_requests:
        if getattr(request, "file_by_filename", None):
            responses.append(get_file_by_filename(request))
        if getattr(request, "file_containing_symbol", None):
            responses.append(get_file_containing_symbol(request))
        if getattr(request, "list_services", None):
            if request.list_services == "services":
                responses.append(SERVICES_MOCK)
            else:
                raise AssertionError(
                    "Listing services for value {} not allowed".format(request.list_services)
                )

    return responses


reflection_client_mock = MagicMock(spec=reflection_pb2_grpc.ServerReflectionStub)
reflection_client_mock.ServerReflectionInfo = server_reflection_info_mock
