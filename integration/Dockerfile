FROM fedora:30

RUN dnf install -y qpid-dispatch-router qpid-dispatch-tools

COPY qdrouterd.conf /etc/qpid-dispatch/
COPY qdrouterd-secure.conf /etc/qpid-dispatch/

RUN useradd --uid 10000 runner
USER 10000

ENTRYPOINT ["qdrouterd"]
