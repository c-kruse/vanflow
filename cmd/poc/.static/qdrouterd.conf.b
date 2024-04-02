router {
    mode: interior
}

connector {
    host: router-a
    port: 5001
    role: inter-router
}

listener {
    host: 0.0.0.0
    port: amqp
    authenticatePeer: no
    saslMechanisms: ANONYMOUS
}

address {
    prefix: mc
    distribution: multicast
}
