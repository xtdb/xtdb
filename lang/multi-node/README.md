
## Requirements

- Client must accept token

- Token should be secret (user shouldn't be able to look at the documentation)

- Client should handle synchronisation on their side, server will be totally asynchronous in handling requests

- Client must set up a node before use and client should tear down after use

## To Go Understand

- How do we set up an HTTP node in code

- Deferring to the XT HTTP server code when connection being handled

## Test Requirements

- Set up two different nodes and show they can process different queries/transactions independently

- Verify routing

## Pseudo-Code Examples

decorator(<number>) -> set up the node for the test, tear down at the end
test1 -- DO transaction to node <number>
