# Run locally with Docker

To use these steps you only need to have Docker installed, no JDK/JVM/Clojure etc. required!

## Pull from Docker Hub and use a persistent volume

```bash
docker pull juxt/crux-standalone-webservice:latest
mkdir my-crux-data & docker run -p 8090:8080 -p 8091:8081 -v $(pwd)/my-crux-data:/usr/src/app/data -i -t juxt/crux-standalone-webservice:latest
```

Once the container is up and running you should see:
```
INFO example-standalone-webservice.main - started HTTP API on port: 8081
INFO example-standalone-webservice.main - started webserver on port: 8080
```

Use the API: `http://localhost:8091`

Or view the demo application: `http://localhost:8090`

# Run locally using Leiningen

```bash

lein repl
example-standalone-webservice.main=> (def s (future
                                #_=>            (run-node
                                #_=>             crux-options
                                #_=>             (fn [_]
                                #_=>               (def crux)
                                #_=>               (Thread/sleep Long/MAX_VALUE)))))
```

Use the API: `http://localhost:8081`

Or view the demo application: `http://localhost:8080`

## Browse the entire transaction log when running in Docker
```bash
curl -X GET http://localhost:8090/tx-log && echo
```
