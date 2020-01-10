# Copyright 2020-present Kensho Technologies, LLC.
from collections import OrderedDict
import unittest

from google.protobuf import any_pb2, wrappers_pb2

from eagr.grpc_utils import response_translation


class TestResponseTranslation(unittest.TestCase):
    def test_translation_of_search_hit_with_primitives(self):
        """Verify that well-known types get translated into their values rather than into dicts."""

        values_and_serializations = (
            (wrappers_pb2.BoolValue(value=True), True),
            (wrappers_pb2.Int32Value(value=1), 1),
            (wrappers_pb2.UInt32Value(value=2), 2),
            # int64 is expected to be converted to a string because of JSON!!
            (wrappers_pb2.Int64Value(value=3), "3"),
            (wrappers_pb2.StringValue(value="abc"), "abc"),
        )

        for value, expected in values_and_serializations:
            any_proto = any_pb2.Any()
            any_proto.Pack(value)
            self.assertEqual(expected, response_translation.translate_message_to_dict(any_proto))

    def test_translation_of_datafixture(self):
        self.maxDiff = None
        raw_response = {
            "displayFieldDescriptors": [
                {
                    "description": "",
                    "fieldName": "profile_link",
                    "fieldType": "Hyperlink",
                    "name": "Company",
                    "subtextFieldName": "",
                    "subtextFieldType": "",
                },
                {
                    "description": "",
                    "fieldName": "geography",
                    "fieldType": "string",
                    "name": "Geography",
                    "subtextFieldName": "",
                    "subtextFieldType": "",
                },
            ],
            "searchHits": [
                {
                    "displayData": {
                        "geography": OrderedDict(
                            [
                                ("@type", "type.googleapis.com/google.protobuf.StringValue"),
                                ("value", "California, United States"),
                            ]
                        ),
                        "profile_link": OrderedDict(
                            [
                                ("@type", "type.googleapis.com/search_structs.Hyperlink"),
                                ("text", "Apple Inc. (XNAS:AAPL)"),
                                ("url", "url"),
                                ("highlights", []),
                            ]
                        ),
                    },
                    "hitId": "4004205",
                },
                {
                    "displayData": {
                        "geography": OrderedDict(
                            [
                                ("@type", "type.googleapis.com/google.protobuf.StringValue"),
                                ("value", "Virginia, United States"),
                            ]
                        ),
                        "profile_link": OrderedDict(
                            [
                                ("@type", "type.googleapis.com/search_structs.Hyperlink"),
                                ("text", "Apple Hospitality REIT, Inc. (XNYS:APLE)"),
                                ("url", "url"),
                                ("highlights", []),
                            ]
                        ),
                    },
                    "hitId": "4187996",
                },
            ],
            "totalResults": 2,
            "verticalName": "companies",
        }

        expected_response = {
            "displayFieldDescriptors": [
                {
                    "description": "",
                    "fieldName": "profile_link",
                    "fieldType": "Hyperlink",
                    "name": "Company",
                    "subtextFieldName": "",
                    "subtextFieldType": "",
                },
                {
                    "description": "",
                    "fieldName": "geography",
                    "fieldType": "string",
                    "name": "Geography",
                    "subtextFieldName": "",
                    "subtextFieldType": "",
                },
            ],
            "searchHits": [
                {
                    "displayData": {
                        "geography": "California, United States",
                        "profile_link": {
                            "highlights": [],
                            "text": "Apple Inc. " "(XNAS:AAPL)",
                            "url": "url",
                        },
                    },
                    "hitId": "4004205",
                },
                {
                    "displayData": {
                        "geography": "Virginia, United States",
                        "profile_link": {
                            "highlights": [],
                            "text": "Apple Hospitality " "REIT, Inc. " "(XNYS:APLE)",
                            "url": "url",
                        },
                    },
                    "hitId": "4187996",
                },
            ],
            "totalResults": 2,
            "verticalName": "companies",
        }

        translated_response = response_translation._get_clean_dict_value(raw_response)
        self.assertEqual(translated_response, expected_response)
