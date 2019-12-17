See basic (non-configured) setup instructions on [docker hub](https://hub.docker.com/r/juxt/crux-http). 

To create a custom configured node, download the
contents of this folder and read the instructions below.

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

## Customizing the node

To customize the node, you can change the configuration options under **crux.edn** and add any further required dependecies 
within **crux.edn**. For example, you can change the KvStore of the node to RocksDB as such (by adding relevant keys under 
`:crux/node-opts`:

```clojure
{:crux/node-opts {:crux.node/topology :crux.standalone/topology
                  :crux.node/kv-store "crux.kv.rocksdb/kv"
                  :crux.kv/db-dir "/var/lib/crux/db"
                  :crux.standalone/event-log-dir "/var/lib/crux/events"
                  :crux.standalone/event-log-kv-store "crux.kv.rocksdb/kv"}
 :crux/server-opts {}}
```

Doing so introduces a dependency to `crux-rocksdb`, which can be added to the docker image by adding it under **deps.edn**:

```clojure
{:deps
 {juxt/crux-http-server {:mvn/version "19.09-1.5.0-alpha"}
  juxt/crux-core {:mvn/version "19.09-1.5.0-alpha"
  juxt/crux-rocksdb {:mvn/version "19.09-1.5.0-alpha"}}}}
```

Now, running the script under `./bin/dev.sh` will create a HTTP node using RocksDB.

For further node configuration options, see the [related section](https://opencrux.com/docs#configuration) within the Crux docs.

## Customizing the HTTP Server

In a similar manner to customizing the node, custom options can be set on the Crux HTTP server which the node is started on. 
For example, to add CORS access permissions to the server, add the following under `:crux/server-opts` before running `./bin/dev.sh`:

```clojure
{:crux/node-opts {:crux.node/topology :crux.standalone/topology
                  :crux.node/kv-store "crux.kv.memdb/kv"
                  :crux.kv/db-dir "/var/lib/crux/db"
                  :crux.standalone/event-log-dir "/var/lib/crux/events"
                  :crux.standalone/event-log-kv-store "crux.kv.memdb/kv"}
 :crux/server-opts 
 {:cors-access-control
   [:access-control-allow-origin [#".*"]
    :access-control-allow-headers ["X-Requested-With"
                                   "Content-Type"
                                   "Cache-Control"
                                   "Origin"
                                   "Accept"
                                   "Authorization"
                                   "X-Custom-Header"]
    :access-control-allow-methods [:get :options :head :post]]}}
```
