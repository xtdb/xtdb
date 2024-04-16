# http-proxy

Allows the setting up and tearing down of multiple xtdb nodes via the HTTP API.

This is to help with automated testing of xtdb clients.

## Building

To build the jar:
```sh
./gradlew :lang:http-proxy:shadowJar
```

To build the docker image (& jar):
```sh
./lang/http-proxy/build.sh
# To force a re-build of the jar:
./lang/http-proxy/build.sh --clean
```

## Usage

### Running

After building the image, you can run it like so:

```sh
docker run -it -p 3300:3300 http-proxy:latest
```

### From a client

With the proxy server running on `http://localhost:3300`:

```
# Setup a new node
GET http://localhost:3300/setup

# You can optionally provide your own token in the query params
GET http://localhost:3300/setup?my-token

# Use the http-proxy url with the token as the base param for requests
base_url = http://localhost/<token>

POST $base_url/query
POST $base_url/tx

# Tear down the node like so
GET http://localhost:3300/teardown?<token>
```

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
