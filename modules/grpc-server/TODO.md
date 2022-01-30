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
- [ ] TX Committed
- [ ] Last Completed TX
- [ ] Last Submitted TX
- [ ] Active Queries
- [ ] Recent Queries
- [ ] Slowest Queries

* Current mapped solutions still need to be tested

## Building protos
1. Download protoc from https://grpc.io
2. `make setup` to download protojure
3. `make protoc` to generate clojure proto namespaces
