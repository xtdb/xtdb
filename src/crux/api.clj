(ns crux.api
  (:require [clojure.tools.logging :as log]
            [crux.bootstrap :as bootstrap]
            [crux.query :as query])
  (:import java.io.Closeable))

(defprotocol CruxSystem
  (db [this] "returns a db for the system"))

(defrecord ApiSystem [close-promise underlying]
  CruxSystem
  (db [_]
    (query/db (:kv-store @underlying)))

  Closeable
  (close [_] (deliver close-promise true)))

(defn q-maps
  [db query]
  (for [item (query/q db query)]
    (into
      {}
      (for [[k v] (meta item)] [(keyword k) (:value v)]))))

(defn ^Closeable start-system
  [options]
  (log/info "running crux in library mode")
  (let [underlying (atom nil)
        close-promise (promise)
        started-promise (promise)
        options (merge bootstrap/default-options options)
        running-future
        (future
          (log/info "crux thread intialized")
          (bootstrap/start-system
            options
            (fn with-system-callback [system]
              (deliver started-promise true)
              (log/info "crux system start completed")
              (reset! underlying system)
              @close-promise
              (log/info "starting teardown of crux system")))
          (log/info "crux system completed teardown"))]
    (while (not (or (deref started-promise 100 false)
                    (deref running-future 100 false))))
    (ApiSystem. close-promise underlying)))
