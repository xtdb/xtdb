(ns dev
  (:require [juxt.crux-ui.server.main :as main]))

(main/-main)

#_(defn reset []
    (.close @main/srv))
