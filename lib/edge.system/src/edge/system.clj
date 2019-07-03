;; Copyright © 2016, JUXT LTD.

(ns edge.system
  "Components and their dependency relationships"
  (:require
   [aero.core :as aero]
   [clojure.java.io :as io]
   [integrant.core :as ig]))

;; There will be integrant tags in our Aero configuration. We need to
;; let Aero know about them using this defmethod.
(defmethod aero/reader 'ig/ref [_ _ value]
  (ig/ref value))

(let [lock (Object.)]
  (defn- load-namespaces
    [system-config]
    (locking lock
      (ig/load-namespaces system-config))))

(defn config
  "Read EDN config, with the given aero options. See Aero docs at
  https://github.com/juxt/aero for details."
  [opts]
  (-> (io/resource "config.edn") ;; <1>
      (aero/read-config opts)) ;; <2>
  )

(defn system-config
  "Construct a new system, configured with the given profile"
  [opts]
  (let [config (config opts) ;; <1>
        system-config (:ig/system config)] ;; <2>
    (load-namespaces system-config) ;; <3>
    (ig/prep system-config) ;; <4>
    ))
