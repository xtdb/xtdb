(ns crux.fixtures.every-api
  (:require [clojure.test :as t]
            [crux.fixtures :as fix :refer [*api*]]
            [crux.fixtures.http-server :as fh]
            [crux.fixtures.jdbc :as fj]
            [crux.fixtures.kafka :as fk]))

(def ^:dynamic *http-server-api* nil)

(def api-implementations
  (-> {:local-standalone fix/with-node
       :remote (t/join-fixtures [fh/with-http-server
                                 fix/with-node
                                 (fn [f] (binding [*http-server-api* *api*] (f)))
                                 fh/with-http-client])
       :h2 (t/join-fixtures [fj/with-h2-opts fj/with-jdbc-node fix/with-node])
       :sqlite (t/join-fixtures [fj/with-sqlite-opts fj/with-jdbc-node fix/with-node])
       :local-kafka (-> (t/join-fixtures [fk/with-cluster-tx-log-opts
                                          fk/with-cluster-doc-store-opts
                                          fix/with-node])
                        (with-meta {::embedded-kafka? true}))
       :local-kafka-transit (-> (t/join-fixtures [(partial
                                                   fk/with-kafka-config
                                                   {:properties-map {"key.serializer" "crux.kafka.transit.TransitSerializer"
                                                                     "key.deserializer" "crux.kafka.transit.TransitDeserializer"
                                                                     "value.serializer" "crux.kafka.transit.TransitSerializer"
                                                                     "value.deserializer" "crux.kafka.transit.TransitDeserializer"}})
                                                  fk/with-cluster-tx-log-opts
                                                  fk/with-cluster-doc-store-opts
                                                  fix/with-node])
                                (with-meta {::embedded-kafka? true}))
       :kafka+remote-doc-store (-> (t/join-fixtures [fk/with-cluster-tx-log-opts fix/with-node])
                                   (with-meta {::embedded-kafka? true}))}
      #_(select-keys [:local-standalone])
      #_(select-keys [:local-standalone :remote])
      #_(select-keys [:local-standalone :h2 :sqlite :remote])))

(def ^:dynamic *node-type*)

(defn with-each-api-implementation [f]
  (doseq [[node-type run-tests] api-implementations]
    (binding [*node-type* node-type]
      (t/testing (str node-type)
        (run-tests f)))))

(defn with-embedded-kafka-cluster [f]
  (if (some (comp ::embedded-kafka? meta) (vals api-implementations))
    (fk/with-embedded-kafka-cluster f)
    (f)))
