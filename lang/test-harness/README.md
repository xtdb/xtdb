# test-harness

This project provides two things:
- A simple test-harness for running tests for the different language clients
- A proxy server for the http module which helps with:
  - setting up nodes
  - interacting with them (querying, submitting txs)
  - and tearing down nodes without re-starting the jvm for each test

## Usage

### Test Harness

To run the tests just run: `./gradlew lang:test-harness:test`

To add a new language to the harness, see the `python_test.clj` for an example.

### The http-proxy

You can run a version locally with `./gradlew lang:test-harness:httpProxy`.
A server is run on port `3300` when the test harness runs.

Once running you can:

```
# Setup a new node
GET http://localhost:3300/setup
# You'll recieve a token in the body of the response

# You can optionally provide your own token in the query params
GET http://localhost:3300/setup?my-token

# Use the http-proxy url with the token as the base param for requests
base_url = http://localhost/<token>

POST $base_url/query
POST $base_url/tx

# Tear down the node like so
GET http://localhost:3300/teardown?<token>
```
