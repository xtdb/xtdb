(ns xtdb.client
  "This namespace is for starting a client for a remote XTDB node.

  It lives in the `com.xtdb/xtdb-http-client-jvm` artifact - ensure you've included this in your dependency manager of choice to use remote clients."
  (:require [xtdb.client.impl :as impl]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn start-client
  "Starts a client that connects to a remote XTDB node"

  ^java.lang.AutoCloseable [url]
  (impl/start-client url))
