(ns crux.topology-info
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [crux.jdbc :as jdbc]
            [crux.standalone :as standalone]
            [crux.node :as node]
            [crux.kafka :as kafka]))

(defn get-topology-info [topology-name]
  (-> (str topology-name "/topology")
      (symbol)
      (eval)))

(get-topology-info 'crux.kafka)
