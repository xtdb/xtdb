(ns crux.fixtures.standalone
  (:require [crux.fixtures.api :as apif]
            [crux.io :as cio]))

(defn with-standalone-node [f]
  (let [event-log-dir (str (cio/create-tmpdir "event-log-dir"))]
    (try
      (apif/with-opts (merge {:crux.bootstrap/node-config :crux.standalone/node-config
                              :crux.standalone/event-log-dir event-log-dir})
        f)
      (finally
        (cio/delete-dir event-log-dir)))))
