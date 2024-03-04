# Tests

## Running Tests

Several tests in this suite can be ran against an external AMQP router. This
project uses the [Qpid Dispatch Router][qdrouterd], which the skupper router
forks. A Dockerfile with appropriate configuration is provided in the
`integration` directory.

### Running full test suite

Start the router container:
`docker run -p 5672:5672 -d --rm --name vanflow-qdr quay.io/ckruse/vanflow-qdr`

Run the tests:
`QDR_ENDPOINT=amqp://127.0.0.1:5672 go test ./...`

Stop router:
`docker stop vanflow-qdr`

### Running without external dependencies

In order to run without the external dependency use the `-short` flag. e.g. `go
test -short ./...`


[qdrouterd]: https://qpid.apache.org/components/dispatch-router/
