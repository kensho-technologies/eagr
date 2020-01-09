# Copyright 2020-present Kensho Technologies, LLC.
import unittest


class ImportTests(unittest.TestCase):
    def test_base_imports(self):
        """Test that the base file imports successfully."""
        from ...server import base  # noqa

    def test_middleware_imports(self):
        """Test that the middleware file imports successfully."""
        from ...server import middleware  # noqa
