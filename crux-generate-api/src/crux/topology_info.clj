(ns crux.topology-info
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [crux.jdbc :as jdbc]
            [crux.standalone :as standalone]
            [crux.node :as node]
            [crux.kafka :as kafka]))

(defn find-nested-args [topology-map]
  (->> (tree-seq map? vals topology-map)
       (filter map?)
       (keep :args)
       (into {})))

(defn format-topology-key [topology-key]
  (-> (name topology-key)
      (string/replace "-" "_")
      (string/upper-case)))

(defn generate-key-strings [topology-key]
  (str "public string " (format-topology-key topology-key) " = \"" topology-key "\""))

(defn get-topology-info [topology-name]
  (let [topology-map (eval topology-name)
        topology-opts (find-nested-args topology-map)]
    topology-opts))
