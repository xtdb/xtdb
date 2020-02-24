(ns crux.soak.main
  (:require [ring.util.response :as ring]
            [ring.adapter.jetty :as jetty]
            [crux.api :as api]))

(def server-id (java.util.UUID/randomUUID))

(def kafka-properties-map {"ssl.endpoint.identification.algorithm" "https"
                           "sasl.mechanism" "PLAIN"
                           "sasl.jaas.config" (format
                                                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";"
                                                (System/getenv "CONFLUENT_API_TOKEN")
                                                (System/getenv "CONFLUENT_API_SECRET"))
                           "security.protocol" "SASL_SSL"})

(defn -main [& args]
  (with-open [node (api/start-node {:crux.node/topology '[crux.kafka/topology]
                                    :crux.kafka/bootstrap-servers (System/getenv "CONFLUENT_SERVER")
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
