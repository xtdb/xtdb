(ns xtdb.node
  "This namespace is for starting an in-process XTDB node.

  It lives in the `com.xtdb/xtdb-core` artifact - ensure you've included this in your dependency manager of choice to use in-process nodes."
  (:require [xtdb.node.impl :as impl]))

(defn start-node
  "Starts an in-process node with the given configuration.

  For a simple, in-memory node (e.g. for testing/experimentation), you can elide the configuration map.

  This node *must* be closed when it is no longer needed (through `.close`, or `with-open`) so that it can clean up its resources.

  For more information on the configuration map, see the relevant module pages in the [ClojureDocs](https://docs.xtdb.com/reference/main/sdks/clojure/index.html)"
  (^java.lang.AutoCloseable [] (start-node {}))

  (^java.lang.AutoCloseable [opts]
   (impl/start-node opts)))

(defn start-submit-client
  "Starts a submit-only client with the given configuration.

  This client *must* be closed when it is no longer needed (through `.close`, or `with-open`) so that it can clean up its resources.

  For more information on the configuration map, see the relevant module pages in the [ClojureDocs](https://docs.xtdb.com/reference/main/sdks/clojure/index.html)"
  ^java.lang.AutoCloseable [opts]
  (impl/start-submit-client opts))
