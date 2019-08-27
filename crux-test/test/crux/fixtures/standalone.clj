(ns crux.fixtures.standalone
  (:require [crux.fixtures.http-server :refer [*api-url*]]
            [crux.fixtures.kv :refer [*kv* *kv-backend*]]
            [crux.fixtures.api :refer [*api*]]
            [crux.io :as cio])
  (:import [crux.api Crux ICruxAPI]))

(defn with-standalone-node [f]
  (assert (not (bound? #'*kv*)))
  (let [db-dir (str (cio/create-tmpdir "kv-store"))
        event-log-dir (str (cio/create-tmpdir "event-log-dir"))]
    (try
      (with-open [standalone-node (Crux/startStandaloneNode {:db-dir db-dir
                                                             :kv-backend *kv-backend*
                                                             :crux.standalone/event-log-kv-backend *kv-backend*
                                                             :event-log-dir event-log-dir})]
        (binding [*api* standalone-node]
          (f)))
      (finally
        (cio/delete-dir db-dir)
        (cio/delete-dir event-log-dir)))))
