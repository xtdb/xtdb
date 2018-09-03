(ns crux.follower-test
  (:require [cheshire.core :as json]
            [clojure.test :as t :refer [deftest is]]
            [crux.embedded-kafka :as ek]
            [crux.fixtures :as f]
            [crux.kafka :as k]
            [crux.query :as q])
  (:import java.util.Date
           [org.apache.kafka.clients.producer KafkaProducer ProducerRecord]
           org.apache.kafka.common.serialization.StringSerializer))

(def follower-topic "follower-topic")

(def ^:dynamic ^KafkaProducer *producer*)

(defn with-follower-options
  [f]
  (k/create-topic ek/*admin-client* follower-topic 1 1 k/tx-topic-config)
  (with-open [producer (k/create-producer
                         {"bootstrap.servers" ek/*kafka-bootstrap-servers*
                          "key.serializer" (.getName StringSerializer)
                          "value.serializer" (.getName StringSerializer)})]
    (binding [f/*extra-options* {:follow-topics {follower-topic "json"}}
              *producer* producer]
      (f))))

(t/use-fixtures :once
  ek/with-embedded-kafka-cluster
  with-follower-options
  f/with-dev-system)

(def ^:dynamic *until-timeout* 10000)

(defn test-until
  [test-f]
  (let [start-time (.getTime (Date.))
        timeout? #(>= (- (.getTime (Date.)) start-time) *until-timeout*)]
    (loop []
      (let [t (test-f)]
        (cond
          t (do (is t) t)
          (timeout?) (is false "tested until timeout")
          :else (do (Thread/sleep 100) (recur)))))))

(defn db [] (q/db (:kv-store f/*system*)))
(defn q [query] (q/q (db) query))

(deftest test-follower
  @(.send *producer*
          (ProducerRecord.
            follower-topic
            "some-key" (json/generate-string
                         {:first-name "Anders"
                          :last-name "Svenson"})))

  (test-until
    #(= #{["Anders" "Svenson"]}
        (q '{:find [f l]
             :where [[e :first-name f]
                     [e :last-name l]]}))))
