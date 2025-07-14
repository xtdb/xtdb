(ns user
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as ctn]
            [xtdb.logging :as logging]
            xtdb.serde
            [xtdb.util :as util])
  (:import java.io.File))

(alter-var-root #'*warn-on-reflection* (constantly true))

(ctn/disable-reload!)
(util/install-uncaught-exception-handler!)
(logging/set-from-env! (System/getenv))

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

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn pgpg []
  (require 'playground)
  (in-ns 'playground))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(defn set-log-level! [ns level]
  (logging/set-log-level! ns level))
