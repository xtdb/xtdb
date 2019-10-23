(ns crux.gen-topology-classes
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [crux.parse-topology-file :as parse]))

(defn gen-topology-class [filename]
  (parse/parse-topology-file filename))

;(gen-topology-class "../crux-core/src/crux/standalone.clj")
