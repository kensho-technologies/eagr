# Expose your GRPC service as a set of "REST"-ish API endpoints

This package allows users to mount any compliant GRPC service as a set of endpoints in a flask app.
Compliant, in this case means service where all endpoints are unary-unary (i.e. streaming is not supported) and take a single argument that should be a protobuf

## Usage

```
from eagr.flask_bridge import grpc_to_json

app = flask.Flask(...)

# Open a channel to the localhost (loopback) to the port that the GRPC service is listening on
local_channel = grpc.insecure_channel('127.0.0.1:{port}'.format(port=port))

grpc_to_json.map_and_mount(app, local_channel, 'fully.qualified.service.name', '/rootMountPoint')

```
