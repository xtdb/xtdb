(ns xtdb.node
  (:require [xtdb.node.impl :as impl]))

(defn start-node ^java.lang.AutoCloseable [opts]
  (impl/start-node opts))

(defn start-submit-node ^java.lang.AutoCloseable [opts]
  (impl/start-submit-node opts))
