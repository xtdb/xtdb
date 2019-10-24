(ns crux.gen-topology-classes
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [crux.topology-info :as ti]))

(defn format-topology-key [key]
  (-> (name key)
      (string/replace "-" "_")
      (string/upper-case)))

(defn generate-key-strings [[key value]]
  (let [keystring (str "public string " (format-topology-key key) " = \"" key "\"")]
    (if (:default value)
      [keystring (str "public string " (format-topology-key key) "_DEFAULT = \"" (:default value) "\"")]
      [keystring])))

(defn gen-topology-class [class-name topology]
  (let [topology-info (ti/get-topology-info topology)]
    (map generate-key-strings (seq topology-info))))

(gen-topology-class "KafkaNode" 'crux.kafka/topology)
