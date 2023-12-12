(ns xtdb.node
  (:require [xtdb.node.impl :as impl]))

(defn start-node
  "Starts an in-process node with the given configuration.

  For a simple, in-memory node (e.g. for testing/experimentation), you can elide the configuration map.

  This node *must* be closed when it is no longer needed (through `.close`, or `with-open`) so that it can clean up its resources.

  For more information on the configuration map, see the relevant module pages in the [ClojureDocs](https://docs.xtdb.com/reference/main/clients/clojure/index.html)"
  (^java.lang.AutoCloseable [] (start-node {}))

  (^java.lang.AutoCloseable [opts]
   (impl/start-node opts)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn start-submit-node
  "Starts a submit-only node with the given configuration.

  This node *must* be closed when it is no longer needed (through `.close`, or `with-open`) so that it can clean up its resources.

  For more information on the configuration map, see the relevant module pages in the [ClojureDocs](https://docs.xtdb.com/reference/main/clients/clojure/index.html)"
  ^java.lang.AutoCloseable [opts]
  (impl/start-submit-node opts))
