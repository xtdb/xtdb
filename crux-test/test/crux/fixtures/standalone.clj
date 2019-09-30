(ns crux.fixtures.standalone
  (:require [crux.fixtures.api :as apif]
            [crux.fixtures.kv :refer [*kv* *kv-backend*]]
            [crux.io :as cio]))

(defn with-standalone-node [f]
  (assert (not (bound? #'*kv*)))
  (let [event-log-dir (str (cio/create-tmpdir "event-log-dir"))]
    (try
      (binding [apif/*opts* {:crux.bootstrap/node-config :crux.standalone/node-config
                             :kv-backend *kv-backend*
                             :crux.standalone/event-log-kv-backend *kv-backend*
                             :event-log-dir event-log-dir}]
        (f))
      (finally
        (cio/delete-dir event-log-dir)))))
