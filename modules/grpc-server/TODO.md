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
`protoc -I=protos/ --java_out=target/generated-sources/protobuf protos/entity.proto protos/service.proto protos/transaction.proto  protos/google/protobuf/any.proto protos/google/protobuf/duration.proto protos/google/protobuf/struct.proto protos/google/protobuf/api.proto protos/google/protobuf/empty.proto protos/google/protobuf/timestamp.proto protos/google/protobuf/field_mask.proto protos/google/protobuf/type.proto protos/google/protobuf/descriptor.proto protos/google/protobuf/source_context.proto protos/google/protobuf/wrappers.proto`