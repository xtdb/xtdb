(ns crux.soak.main
  (:require [ring.util.response :as resp]
            [ring.adapter.jetty :as jetty]
            [crux.api :as api]))

(def server-id (java.util.UUID/randomUUID))

(def soak-secrets
  (-> (System/getenv "SOAK_SECRETS")
      (json/decode)))

(def kafka-properties-map
  {"ssl.endpoint.identification.algorithm" "https"
   "sasl.mechanism" "PLAIN"
   "sasl.jaas.config" (str (->> ["org.apache.kafka.common.security.plain.PlainLoginModule"
                                 "required"
                                 (format "username=\"%s\"" (get soak-secrets "CONFLUENT_API_TOKEN"))
                                 (format "password=\"%s\"" (get soak-secrets "CONFLUENT_API_SECRET"))]
                                (string/join " "))
                           ";")
   "security.protocol" "SASL_SSL"})

(defn -main [& args]
  (with-open [node (api/start-node {:crux.node/topology '[crux.kafka/topology]
                                    :crux.kafka/bootstrap-servers (get soak-secrets "CONFLUENT_BROKER")
                                    :crux.node/kv-store 'crux.kv.memdb/kv
                                    :crux.kafka/replication-factor 3
                                    :crux.kafka/doc-partitions 6
                                    :crux.kafka/kafka-properties-map kafka-properties-map})]

    (let [handler (fn [request]
                    (api/submit-tx node [[:crux.tx/put
                                          {:crux.db/id (keyword (:remote-addr request))
                                           :type :visitor}]])
                    (-> (ring/response (str {:server-id server-id
                                             :body (slurp (:body request))
                                             :visitor-count (count (api/q
                                                                     (api/db node)
                                                                     {:find ['e]
                                                                      :where [['e :type :visitor]]}))}))
                        (ring/content-type "text/plain")))]
      (jetty/run-jetty handler {:port 8080}))))
