# Copyright 2020-present Kensho Technologies, LLC.
import unittest

from ...server.middleware import MetricsMiddleware


class TestMiddlewares(unittest.TestCase):
    def test_MetricsMiddleware_get_decorator_naming(self):
        metrics_middleware = MetricsMiddleware()

        # Assert that no exception is raised
        metrics_middleware.get_decorator("/in/need.of_normalization", {})

        with self.assertRaises(AssertionError):
            metrics_middleware.get_decorator("-no_dash_at_start", {})
