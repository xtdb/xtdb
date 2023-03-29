(ns user
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as ctn]
            xtdb.edn
            [xtdb.util :as util])
  (:import java.io.File))

(alter-var-root #'*warn-on-reflection* (constantly true))

(ctn/disable-reload!)
(util/install-uncaught-exception-handler!)

(apply ctn/set-refresh-dirs (for [^File dir (concat [(io/file ".")]
                                                    (.listFiles (io/file "."))
                                                    (.listFiles (io/file "modules")))
                                  :when (and (.isDirectory dir)
                                             (.exists (io/file dir "deps.edn")))
                                  sub-dir #{"src/main/clojure" "src/test/clojure"}
                                  :let [refresh-dir (io/file dir sub-dir)]

                                  :when (not= refresh-dir (io/file "modules/flight-sql/src/test/clojure"))]
                              refresh-dir))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn reset []
  (ctn/refresh))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn dev []
  (require 'dev)
  (in-ns 'dev))

