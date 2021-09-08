(ns user
  (:require [clojure.java.io :as io]
            [clojure.tools.namespace.repl :as ctn])
  (:import java.io.File))

(ctn/disable-reload!)
(apply ctn/set-refresh-dirs (for [^File dir (.listFiles (io/file "."))
                                  :when (and (.isDirectory dir)
                                             (not (-> dir .getName #{"graal"})))
                                  sub-dir #{"src" "test"}]
                              (io/file dir sub-dir)))

(defn dev []
  (require 'dev)
  (in-ns 'dev))
