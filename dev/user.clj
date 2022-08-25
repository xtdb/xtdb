(ns user
  (:require [clojure.tools.namespace.repl :as ctn]
            [clojure.java.io :as io]
            core2.edn
            [time-literals.read-write :as time-literals])
  (:import java.io.File))

(alter-var-root #'*warn-on-reflection* (constantly true))
(ctn/disable-reload!)

(apply ctn/set-refresh-dirs (cons (io/file "test")
                                  (for [^File dir (concat (.listFiles (io/file "."))
                                                          (.listFiles (io/file "modules")))
                                        :when (and (.isDirectory dir)
                                                   (.exists (io/file dir "deps.edn")))
                                        sub-dir #{"src" "test"}]
                                    (io/file dir sub-dir))))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn reset []
  (ctn/refresh))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn dev []
  (require 'dev)
  (in-ns 'dev))

(time-literals/print-time-literals-clj!)
