(ns crux.topology-info
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [crux.jdbc :as jdbc]
            [crux.standalone :as standalone]
            [crux.node :as node]
            [crux.kafka :as kafka]))

(defn get-topology-map [topology-name]
  (-> (str topology-name "/topology")
      (symbol)
      (eval)))

(defn get-topology-info [topology-name]
  (get-topology-info topology-name))
