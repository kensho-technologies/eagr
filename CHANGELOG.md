Changelog
=========

### v0.2.0

* Method invocations returned for the GRPC Reflection Interface now accept parameters for `timeout`, `metadata`, `credentials`, `wait_for_ready`, and `compression`, matching the full contract for `UnaryUnaryMultiCallable` objects.

### v0.1.1

* Fixed broken GRPC Reflection Interface. Now instantiates a reflection client as expected.
* Updated the README with shields and install instructions
* Added Poetry lock file

### v0.1.0

Initial release.
