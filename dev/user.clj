(ns user
  (:require [clojure.tools.namespace.repl :as ctn]
            [clojure.java.io :as io])
  (:import java.io.File))

(set! *warn-on-reflection* true)
(ctn/disable-reload!)

(apply ctn/set-refresh-dirs (for [^File dir (concat (.listFiles (io/file "."))
                                                    (.listFiles (io/file "modules")))
                                  :when (and (.isDirectory dir)
                                             (.exists (io/file dir "deps.edn")))
                                  sub-dir #{"src" "test"}]
                              (io/file dir sub-dir)))

(defn reset []
  (ctn/refresh))

(defn dev []
  (require 'dev)
  (in-ns 'dev))
