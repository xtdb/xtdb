(ns build
  (:require [clojure.tools.build.api :as b]))

(def basis (b/create-basis {:project "deps.edn"}))

(defn prep [_opts]
  (b/process {:command-args ["clojure" "-M:generate-sql-parser"]})

  (b/javac {:basis basis
            :src-dirs ["src"]
            :class-dir "target/classes"}))
