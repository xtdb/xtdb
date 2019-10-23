(ns crux.gen-topology-classes
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [crux.topology-info :as ti]))

(defn gen-topology-class [topology]
  (ti/get-topology-info topology))

(gen-topology-class 'crux.kafka)
