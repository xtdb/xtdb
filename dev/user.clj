(ns user
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as ctn]))

(doseq [ns '[user core2.compute.simd core2.temporal.simd]]
  (ctn/disable-reload! (create-ns ns)))

(defn reset []
  (ctn/refresh))
