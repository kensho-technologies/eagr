# Copyright 2020-present Kensho Technologies, LLC.
from typing import Any, Dict

from google.protobuf import json_format


POD_TYPES = (int, float, str)
TYPE_KEY = "@type"
TYPE_KEY_SET = frozenset({TYPE_KEY})


def _get_clean_value(value):
    # type: (Any) -> Any
    """Determine which cleaner to use for the given value, and apply it."""
    if isinstance(value, dict):
        return _get_clean_dict_value(value)
    elif isinstance(value, list):
        return _get_clean_list_value(value)
    elif isinstance(value, POD_TYPES):  # Immutable built-in types.
        return value
    else:
        raise AssertionError(u"Cannot clean value of type {}: {}".format(type(value), value))


def _get_clean_dict_value(dct):
    # type: (Dict) -> Any
    """Clean up a dictionary which represents an encoded Any type.

    A dictionary can arise in 2 different ways when translating a message to a dictionary (via
    MessageToDict).

    The underlying data structure map be a dictionary (protobuf) or else the type that is being
    translated is an Any type. While translating an Any type the translator produces a dict
    that multiple keys one of which is a @type key that denotes the type of the message being
    translated.

    Examples:
        Any(Int32(value=3))

        Gets translated into a raw message of:

        {
            '@type': 'google...int32',
            'value': 3,
        }

        The desired output from this function is to replace the dictionary with the integer value 3.

        Any(SomeStructure(a=2, b=3))

        {
            '@type': 'google...some_structure',
            'a': 2,
            'b': 3
        }

        The desired output from this function is to retain the dictionary structure and instead
        simply remove the @type key. So

        {
            'a': 2,
            'b': 3
        }

    Args:
        dct: dict instance, as outputted from MessageToDict.

    Returns:
        dict, clean version of dict
    """
    keys = set(dct.keys())

    is_flat_any_type = TYPE_KEY in keys and len(keys) == 2

    if not is_flat_any_type:
        return _project_dict_without_type(dct)
    else:
        remaining_keys = set(keys) - TYPE_KEY_SET

        if len(remaining_keys) != 1:
            raise ValueError(
                u"Expected to find @type in the dict and one another key "
                u"Instead found {} for other keys.".format(remaining_keys)
            )

        remaining_key = list(remaining_keys)[0]
        value = dct[remaining_key]
        return _get_clean_value(value)


def _project_dict_without_type(dct):
    # type (Dict) -> Any
    """Clean a dict value.

    Args:
        dct: dict

    Returns:
        Any, any value to replace the dictionary with.
    """
    return {key: _get_clean_value(value) for key, value in dct.items() if key != TYPE_KEY}


def _get_clean_list_value(iterable):
    # type (List) -> Any
    """Clean a list iterable.

    Args:
        iterable: iterable

    Returns:
        Any, any value to replace the list with.
    """
    return [_get_clean_value(element) for element in iterable]


#
#  PUBLIC API
#


def translate_message_to_dict(message):
    # type (Any) -> Dict
    """Translate protobuf message to dictionary.

    Args:
        message: The protocol buffer message to be translated

    Returns:
        dict, clean version of the message in jsonifiably dict format.
    """
    # The default value is included here to make the response to the UI such that there are no
    # keys missing randomly. If this is changed, the UI wil have to start handling randomly
    # missing keys and the code that produces Any type conversion will convert Any types
    # inconsistently. Primitive Any types are flattened, while Any types that are structs
    # are kept as dicts currently with the logic relying on the number of keys.
    raw_response = json_format.MessageToDict(message, including_default_value_fields=True)
    simplified_dict = _get_clean_dict_value(raw_response)

    return simplified_dict
