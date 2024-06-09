server {
    host = '0.0.0.0'
    port = 8080
    ioWorkersCount = 1
    ttl = 60 * 1000
    context = '/'
    debugOutput = true

    session {
        ttl = 30 * 60
    }
    websocketFrameLengthLimit = 10 * 1024 * 1024
}

datasource {
    url = 'jdbc:postgresql://localhost:5435/metricsdb'
    username = 'metrics-rw'
    password = 'qwerty'
}