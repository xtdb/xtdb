(ns crux.api
  (:require [clojure.tools.logging :as log]
            [crux.bootstrap :as bootstrap])
  (:import java.io.Closeable))

(defn ^Closeable start-system
  [options]
  (let [close-promise (promise)
        options (merge bootstrap/default-options options)]
    (log/info "running crux in library mode")
    (doto (Thread.
            (fn library-crux-thread []
              (log/info "crux thread intialized")
              (bootstrap/start-system
                options
                (fn with-system-callback [system]
                  (log/info "crux system start completed")
                  @close-promise
                  (log/info "starting teardown of crux system")))
              (log/info "crux system completed teardown")))
      (.start))
    (reify Closeable
      (close [_] (deliver close-promise true)))))
