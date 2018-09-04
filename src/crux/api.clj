(ns crux.api
  (:require [clojure.tools.logging :as log]
            [crux.bootstrap :as bootstrap])
  (:import java.io.Closeable))

(defn ^Closeable start-system
  [options]
  (log/info "running crux in library mode")
  (let [close-promise (promise)
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
              @close-promise
              (log/info "starting teardown of crux system")))
          (log/info "crux system completed teardown"))]
    (while (not (or (deref started-promise 100 false)
                    (deref running-future 100 false))))
    (reify Closeable
      (close [_] (deliver close-promise true)))))
