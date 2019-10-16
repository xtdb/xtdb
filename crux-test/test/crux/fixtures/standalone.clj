(ns crux.fixtures.standalone
  (:require [crux.fixtures.api :as apif]
            [crux.io :as cio]))

(defn with-standalone-node [f]
  (let [event-log-dir (str (cio/create-tmpdir "event-log-dir"))]
    (try
      (apif/with-opts {:crux.node/topology :crux.standalone/topology
                       :crux.standalone/event-log-dir event-log-dir}
        f)
      (finally
        (cio/delete-dir event-log-dir)))))
