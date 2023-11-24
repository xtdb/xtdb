(ns xtdb.client
  (:require [xtdb.client.impl :as impl]))

(defn start-client ^java.lang.AutoCloseable [url]
  (impl/start-client url))
