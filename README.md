# Eagr

Easy and Abstracted gRPC. A collection of utilities for making GRPC easier to use in python. This package is widely used within Kensho.

## Server

Functionality to simplify running grpc services. Provides a common set of metrics and logging


### Sample usage:

```python
import time
from eagr import (
    GRPCBase,
    run_grpc_servers,
)


# UserServicer is from generated proto
class UserService(GRPCBase, UserServicer):
    # Set to generated proto add_<>_to_server function
    _REGISTRAR = None

    def Create(self, request, context):
        return User(name='louis')


def start_server():
    user_service = UserService()
    with run_grpc_servers((user_service,), grpc_port=9000, metrics_port=9001):
        while True:
            print("Server running.")
            time.sleep(100)
```


## Client

Functionality to simplify instantiating a client as well as wrapping it with metrics, logging, etc

### Sample usage:

```python
from eagr import make_grpc_client

from your_service_pb2_gprc import YourServiceStub


client = make_grpc_client(
    "client group for metrics", "service name", "service url", YourServiceStub
)
```


## REST Passthrough

[Functionality to bind unary grpc methods to Flask handlers](eagr/flask_bridge/Readme.md)


## Unittest Server


Functionality to create an in-process server for unittesting


### Sample usage:


```python
import unittest
from unittest.mock import MagicMock

from eagr import inprocess_grpc_server, make_grpc_client

from your_service_pb2_gprc import YourServicer, YourServiceStub, add_YourServicer_to_server


class TestMyClient(unittest.TestClient):

    def test_something(self):
        mock_servicer = MagicMock(YourServicer)
        mock_servicer.FooBar = lambda x, _: x
        with inprocess_grpc_server(mock_servicer, add_YourServicer_to_server) as address:
            client = make_grpc_client("foo", "bar", address, YourServiceStub)
            baz = <your input here>
            self.assertEqual(baz, client.FooBar(baz))
```


## GRPC Reflection Interface

Functionality to create a generic GRPC client based on a GRPC server that has reflection enabled

### Sample usage:

```python
from eagr.reflection import grpc_reflection_interface


host = "myhost"
service = "MyService"
params = {
    "keyfoo": "valuebar"
}
generic_myservice_client = grpc_reflection_interface.make_json_grpc_client(host, service)

my_grpc_response = generic_myservice_client["my_grpc_method"](params)
```


# License

Licensed under the Apache 2.0 License. Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

Copyright 2020-present Kensho Technologies, LLC. The present date is determined by the timestamp of the most recent commit in the repository.
