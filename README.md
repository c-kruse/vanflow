# vanflow [![Godoc](https://godoc.org/github.com/c-kruse/vanflow?status.svg)](https://godoc.org/github.com/c-kruse/vanflow)
vanflow is a Go library implementing the skupper.io Vanflow protocol.

## What's included

* The `vanflow` root package defines the messages and record types defined in
  the vanflow specification (unpublished version 0.1.)
* The `encoding` package provides utilities for converting between amqp value
  maps and the record types.
* The `eventsource` package is used to discover, interact with, and to
  implement vanflow event sources.
* The `session` package is used internally by `eventsource` to simplify
  connection and session management.
* The **still very experimental** `store` package provides a way to store and access
  vanflow record information.

### Executables

The `vanflow-tool` command is a rough utility for logging vanflow messages, for
capturing and replaying VAN state.

