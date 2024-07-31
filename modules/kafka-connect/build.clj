(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'com.xtdb/xtdb-kafka-connect)
(def version "2.0.0-SNAPSHOT")
(def class-dir "target/classes")
(def uber-file (format "target/%s-%s-standalone.jar" (name lib) version))

(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn compile-java [_]
  (b/javac {:basis @basis
            :src-dirs ["src"]
            :class-dir class-dir
            :javac-opts ["--release" "11"]}))

(defn compile-clj [_]
  (b/compile-clj {:basis @basis
                  :src-dirs ["src"]
                  :class-dir class-dir}))

(defn uber [_]
  (compile-java nil)
  (compile-clj nil)
  (b/copy-dir {:src-dirs ["resources"]
               :target-dir class-dir})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis @basis}))
