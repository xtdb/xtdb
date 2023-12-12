(ns xtdb.client
  (:require [xtdb.client.impl :as impl]))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn start-client
  "Starts a client that connects to a remote XTDB node"

  ^java.lang.AutoCloseable [url]
  (impl/start-client url))
