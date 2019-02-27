## Run locally using Docker and a persistent volume

```bash
docker pull juxt/crux-standalone-webservice:latest
mkdir my-crux-data & docker run -p 8079:8080 -v $(pwd)/my-crux-data:/usr/src/app/data -i -t juxt/crux-standalone-webservice:latest
```

Navigate to: `http://localhost:8079`

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
