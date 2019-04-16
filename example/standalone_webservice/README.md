# Run locally with Docker

To use these steps you only need to have Docker installed, no JDK/JVM/Clojure etc. required!

## Pull from Docker Hub and use a persistent volume

```bash
docker pull juxt/crux-standalone-webservice:latest
mkdir my-crux-data & docker run -p 8079:8080 -v $(pwd)/my-crux-data:/usr/src/app/data -i -t juxt/crux-standalone-webservice:latest
```

Navigate to: `http://localhost:8079`

# Run locally using Leiningen

```bash

lein repl
example-standalone-webservice.main=> (def s (future
                                #_=>            (run-system
                                #_=>             crux-options
                                #_=>             (fn [_]
                                #_=>               (def crux)
                                #_=>               (Thread/sleep Long/MAX_VALUE)))))
```

Navigate to: `http://localhost:8080`

# Use the ephemeral sandbox hosted by JUXT

## Run the posts query against the hosted sandbox node
```bash
curl -X POST \
     -H "Content-Type: application/edn" \
     -d '{:query {:find [m n c e ed]
                  :where [[e :message-post/message m]
                          [e :message-post/name n]
                          [e :message-post/created c]
                          [e :message-post/edited ed]]}}' \
     http://sandbox.crux.cloud:3000/query && echo
```

## Get entire transaction log
```bash
curl -X GET http://sandbox.crux.cloud:3000/tx-log && echo
```
