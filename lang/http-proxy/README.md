
## Requirements

- Client must accept token

- Token should be secret (user shouldn't be able to look at the documentation)

- Client should handle synchronisation on their side, server will be totally asynchronous in handling requests

- Client must set up a node before use and client should tear down after use

## Test Requirements

- Verify routing
- Mock requests

## Pseudo-Code Examples

decorator(<number>) -> set up the node for the test, tear down at the end
test1 -- DO transaction to node <number>
