(ns user
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as ctn]
            xtdb.serde
            [xtdb.util :as util]
            [xtdb.xray :as xray :refer [xray]])
  (:import java.io.File))

(alter-var-root #'*warn-on-reflection* (constantly true))

(ctn/disable-reload!)
(util/install-uncaught-exception-handler!)

(apply ctn/set-refresh-dirs (conj (for [^File dir (concat [(io/file ".")]
                                                          (.listFiles (io/file "."))
                                                          (.listFiles (io/file "modules")))
                                        :when (and (.isDirectory dir)
                                                   (.exists (io/file dir "build.gradle.kts")))]
                                    (io/file dir "src/main/clojure"))
                                  "src/test/clojure"))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn reset []
  (ctn/refresh))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn dev []
  (require 'dev)
  (in-ns 'dev))
