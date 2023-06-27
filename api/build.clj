(ns build
  (:require [clojure.tools.build.api :as b]))

(def basis (b/create-basis {:project "deps.edn"}))

(defn prep [_opts]
  (b/javac {:basis basis
            :src-dirs ["src"]
            :javac-opts ["-source" "17" "-target" "17"
                         "-XDignore.symbol.file"
                         "-Xlint:all,-options,-path"
                         "-Werror"
                         "-proc:none"]
            :class-dir "target/classes"}))
