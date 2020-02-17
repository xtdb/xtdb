If you wish to use Crux in Docker, you have two options:
- Crux can be embedded within your JVM application - you don't need our Docker images for this, you can include Crux as a library and deploy it as you would any other JVM Docker application (see the [docs](https://opencrux.com/docs))
- If you want to access Crux via HTTP, you want this Docker image, instructions below.

The following assumes that you are copying the contents of this folder and intend to create your own node configuration. If you wish to see how to **interact** with a basic, non-configured setup, check out the overview on [docker hub](https://hub.docker.com/r/juxt/crux-http).

---
## Getting Started

The only requirement for setting up this node is **docker**, so ensure that is installed. Firstly, if you have not already done so,
download the most recent image of the http node from docker hub:

```bash
docker pull juxt/crux-http
```

To run the basic instance of the node (which runs using the **memdb**), run the script within `./bin/dev.sh`. This image makes use of
some default Crux node / Crux HTTP Server options defined within the file **crux.edn**, and the dependencies within **deps.edn** to
start a HTTP node running on **localhost:3000**.

### Starting nREPL/pREPL Server

Optionally, you can start an nREPL/pREPL server on **localhost:7888** alongside your HTTP node by adding either "--nrepl" or "--prepl" within **docker-compose.dev.yml** under `command:`.


## Customizing the node

To customize the node, you can change the configuration options under **crux.edn** and add any further required dependecies
within **crux.edn**. For example, you can change the KvStore of the node to RocksDB as such (by adding relevant keys under
`:crux/node-opts`:

```clojure
{:crux/node-opts {:crux.node/topology ['crux.standalone/topology]
                  :crux.node/kv-store 'crux.kv.rocksdb/kv
                  :crux.kv/db-dir "/var/lib/crux/db"}
 :crux/server-opts {}}
```

Doing so introduces a dependency to `crux-rocksdb`, which can be added to the docker image by adding it under **deps.edn**:

```clojure
{:deps
 {juxt/crux-http-server {:mvn/version "20.01-1.6.2-alpha"}
  juxt/crux-core {:mvn/version "20.01-1.6.2-alpha"
  juxt/crux-rocksdb {:mvn/version "20.01-1.6.2-alpha"}}
 ...}}
```

Now, running the script under `./bin/dev.sh` will create a HTTP node using RocksDB.

**For further node configuration options, see the [related section](https://opencrux.com/docs#configuration) within the Crux docs.**

## Customizing the HTTP Server

In a similar manner to customizing the node, custom options can be set on the Crux HTTP server which the node is started on.
For example, to add CORS access permissions to the server, add the following under `:crux/server-opts` before running `./bin/dev.sh`:

```clojure
{:crux/node-opts {...}
 :crux/server-opts
 {:cors-access-control
   {:access-control-allow-origin [#".*"]
    :access-control-allow-methods [:get :options :head :post]}}}
```

## Customizing Logging Configuration

Within the resources directory, a logging configuration file `logback.xml`, is included. You can edit the contents of the file and run `./bin/dev.sh` to change logging options.

---

### Updating / Redeploying the docker image

To rebuild / redeploy the docker image, ensure you are logged into docker and have access to the **juxt** docker account, and run the `./bin/build.sh`/`./bin/tag.sh`/`./bin/push.sh` scripts. Updating Crux to a new version requires updating the versions within `deps.edn`, and changing the tag version within `tag.sh`/`push.sh`.
