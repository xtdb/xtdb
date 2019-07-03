;; Copyright © 2016, JUXT LTD.

(ns edge.test.system
  (:require
   [integrant.core :as ig]
   [edge.system]))

(def ^:dynamic *system* nil)

(defmacro ^:private with-system
  [system ks & body]
  `(let [system# ~system
         s# (ig/init system# (or ~ks (keys system#)))]
     (try
       (binding [*system* s#]
         ~@body)
       (finally
         (ig/halt! s#)))))

(defn- default-system
  []
  (edge.system/system-config
    {:profile :test}))

(defn with-system-fixture
  ([]
   (with-system-fixture default-system))
  ([system]
   (fn [f]
     (with-system (system) nil
       (f)))))

(defn with-subsystem-fixture
  ([ks]
   (with-subsystem-fixture default-system))
  ([system ks]
   (fn [f]
     (with-system (system) ks
       (f)))))
