(ns sys
  (:require [clojure.tools.namespace.repl :as tn]
            [clojure.tools.logging :as log])
  (:import [clojure.lang IDeref]
           [java.io Closeable]))

;; Inspired by
;; https://medium.com/@maciekszajna/reloaded-workflow-out-of-the-box-be6b5f38ea98

(def instance)
(def init)

(defn ^Closeable closeable [value close-fn]
  (reify
    IDeref
    (deref [_]
      value)
    Closeable
    (close [_]
      (close-fn value))))

(defn start []
  (alter-var-root
   #'instance (fn [instance]
                (cond
                  (not (bound? #'init))
                  (throw (IllegalStateException. "init not set."))

                  (instance? Closeable instance)
                  (throw (IllegalStateException. "Already running."))

                  :else
                  (cast Closeable (init)))))
  :started)

(defn stop []
  (when (and (bound? #'instance)
             (not (nil? instance)))
    (alter-var-root #'instance #(.close ^Closeable %)))
  :stopped)

(defn clear []
  (alter-var-root #'instance (constantly nil)))

(defn reset []
  (stop)
  (let [result (tn/refresh :after 'sys/start)]
    (if (instance? Throwable result)
      (throw result)
      result)))

(defn make-init-fn
  [system-var create-system]
  (fn []
    (let [started-promise (promise)
          close-promise (promise)
          instance (closeable
                    (future
                      (try
                        (create-system
                          (fn [started-system]
                            (alter-var-root system-var (constantly started-system))
                            (deliver started-promise true)
                            @close-promise))
                        (catch Throwable t
                          (log/error t "Exception caught, system will stop:")
                          (throw t))
                        (finally
                          (alter-var-root system-var (constantly nil)))))
                    (fn [this]
                      (deliver close-promise true)
                      @this))]
      (while (not (or (deref @instance 100 false)
                      (deref started-promise 100 false))))
      instance)))
