# Development Process

## Service API mapped to Protobuf3 (~Analogous to HTTP)
- [x] Status
- [x] Entity
- [x] Entity History
- [ ] Entity TX
- [ ] Query
- [ ] Attribute Stats
- [ ] Sync
- [ ] Await TX (Stream?)
- [ ] Await TX Time (Stream?)
- [ ] TX log
- [x] Submit TX
    - [x] Put
    - [x] Delete
    - [ ] Match
    - [x] Evict
    - [ ] Function
- [ ] TX Committed
- [ ] Last Completed TX
- [ ] Last Submitted TX
- [ ] Active Queries
- [ ] Recent Queries
- [ ] Slowest Queries

* Current mapped solutions still need to be tested

## Example requests:
### Status
`{}`

### Submit-tx
```json
{
  "txOps": [
    {
      "put": {
        "document": {
          "xt/id": "foofoo",
          "hello": "world"
        },
        "validTime": "1970-01-01T00:02:03.000000123Z"
      }
    },
    {
      "delete": {
        "documentId": "foofoo",
        "validTime":"1970-01-01T00:02:03.000000123Z",
        "endValidTime":"1970-02-01T00:02:03.000000123Z"
        
      }, 
    },
    {
      "evict": {
        "documentId": "foofoo",
      },
    }
  ]
}
```

## Building protos
1. Download protoc from https://grpc.io
2. `make setup` to download protojure
3. `make protoc` to generate clojure proto namespaces
