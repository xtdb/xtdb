(ns user
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as ctn]
            core2.edn
            [core2.util :as util]
            [time-literals.read-write :as time-literals])
  (:import java.io.File))

(alter-var-root #'*warn-on-reflection* (constantly true))

(ctn/disable-reload!)
(util/install-uncaught-exception-handler!)

(apply ctn/set-refresh-dirs (for [^File dir (concat [(io/file ".")]
                                                    (.listFiles (io/file "."))
                                                    (.listFiles (io/file "modules")))
                                  :when (and (.isDirectory dir)
                                             (.exists (io/file dir "deps.edn")))
                                  sub-dir #{"src/main/clojure" "src/test/clojure"}]
                              (io/file dir sub-dir)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn reset []
  (ctn/refresh))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn dev []
  (require 'dev)
  (in-ns 'dev))

(time-literals/print-time-literals-clj!)
