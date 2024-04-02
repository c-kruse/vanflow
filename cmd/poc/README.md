# POC simulation for adding sequence numbers to vanflow

This executable is a simulation to help us understand how adding a kuberentes
watch api inspired "resourceVersion" in vanflow might end up working.

## Changes

Instead of versioning individual records, for this simulation I added a
`sequence` application property to Record messages and a `head` application
property to Flush and Heartbeat messages. The event source sends Record
messages with an incrementing sequence number and needs to keep track of the
most recent message sequence number a record was included in (including
terminated records) for the lifetime of the event source. All heartbeat
messages contain `head` indicating the most recently sent Record message's
`sequence`.

The collector tracks the sequence number, and when a heartbeat message's `head`
doesn't match up, or a record message `sequence` is not the expected next
value, the collector enters "flush mode", sends a flush message, and processes
all record messages from that source regardless of its sequence number until it
receives a heartbeat, at which time it resets its copy of the sequence number
to `head` from that heartbeat and return to "normal mode" where it will
continue to track sequence number.


## Running

Use the docker/podman compose project to start two qpid dispatch routers.

Run the tool pointing source and collector servers at separate instances. It
should start an event source that makes random changes to a set of Process
records a few times per second, and a collector that tries to keep pace. When
interrupted (^C) the event source will stop the random mutations. A second
interrupt will cause the source and collector to shut down and their states
will be compared for differences before exiting.
`poc -source-server amqp://127.0.0.1:15672 -collector-server amqp://127.0.0.1:25672`

Try breaking a connection and waiting for an error to be logged (or for `qdstat
-c` to indicate the connection is gone) before undoing it. The collector should
eventually recover.
`sudo iptables -I OUTPUT -d 127.0.0.1 -p tcp --dport 25672 -j DROP && sleep 30
&& sudo iptables -D OUTPUT -d 127.0.0.1 -p tcp --dport 25672 -j DROP` (or if
you're podman network savvy enough to disconnect the two router containers that
may be interesting too)

