
## Run the posts query against the sandbox node
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
