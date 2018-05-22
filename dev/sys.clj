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

(defn with-system-var [do-with-system-fn target-var]
  (fn [system]
    (try
      (alter-var-root target-var (constantly system))
      (do-with-system-fn system)
      (finally
        (alter-var-root target-var (constantly nil))))))

(defn with-system-promise [do-with-system-fn promise]
  (fn [system]
    (deliver promise system)
    (do-with-system-fn system)))

(defn make-init-fn [with-system-fn do-with-system-fn close-fn]
  (fn []
    (let [started? (promise)
          instance (closeable
                    (future
                      (try
                        (-> do-with-system-fn
                            (with-system-promise started?)
                            (with-system-fn))
                        (catch Throwable t
                          (log/error t "Exception caught, system will stop:")
                          (throw t))))
                    (fn [this]
                      (close-fn this)
                      @this))]
      (while (not (or (deref @instance 100 false)
                      (deref started? 100 false))))
      instance)))
