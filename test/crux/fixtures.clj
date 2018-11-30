(ns crux.fixtures
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [crux.api :as api]
            [crux.bootstrap :as b]
            [crux.db :as db]
            [crux.io :as cio]
            [crux.kafka.embedded-kafka :as ek]
            [crux.http-server :as srv]
            [crux.kafka :as k]
            [crux.kv-store :as ks]
            [crux.tx :as tx])
  (:import java.io.Closeable
           [java.util Properties UUID]
           org.apache.kafka.clients.admin.AdminClient
           org.apache.kafka.clients.producer.KafkaProducer
           org.apache.kafka.clients.consumer.KafkaConsumer))

(defn random-person [] {:crux.db/id (UUID/randomUUID)
                        :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
                        :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
                        :sex       (rand-nth [:male :female])
                        :age       (rand-int 100)
                        :salary    (rand-int 100000)})

(defn maps->tx-ops [maps ts]
  (vec (for [m maps]
         [:crux.tx/put
          (:crux.db/id m)
          m
          ts])))

(defn transact-entity-maps!
  ([kv entities]
   (transact-entity-maps! kv entities (cio/next-monotonic-date)))
  ([kv entities ts]
   (let [tx-log (tx/->DocTxLog kv)
         tx-ops (maps->tx-ops entities ts)]
     @(db/submit-tx tx-log tx-ops)
     entities)))

(defn entities->delete-tx-ops [entities ts]
  (vec (for [e entities]
         [:crux.tx/delete e ts])))

(defn delete-entities!
  ([kv entities]
   (delete-entities! kv entities (cio/next-monotonic-date)))
  ([kv entities ts]
   (let [tx-log (tx/->DocTxLog kv)
         tx-ops (entities->delete-tx-ops entities ts)]
     @(db/submit-tx tx-log tx-ops)
     entities)))

(defn transact-people!
  ([kv people-mixins]
   (transact-people! kv people-mixins (cio/next-monotonic-date)))
  ([kv people-mixins ts]
   (transact-entity-maps! kv (->> people-mixins (map merge (repeatedly random-person))) ts)))

(def ^:dynamic *kv*)
(def ^:dynamic *kv-backend* "crux.rocksdb.RocksKv")

(defn with-kv-store [f]
  (let [db-dir (cio/create-tmpdir "kv-store")]
    (try
      (binding [*kv* (b/start-kv-store {:db-dir db-dir :kv-backend *kv-backend*})]
        (with-open [*kv* ^Closeable *kv*]
          (f)))
      (finally
        (cio/delete-dir db-dir)))))

(defn with-memdb [f]
  (binding [*kv-backend* "crux.memdb.MemKv"]
    (t/testing "MemDB"
      (f))))

(defn with-rocksdb [f]
  (binding [*kv-backend* "crux.rocksdb.RocksKv"]
    (t/testing "RocksDB"
      (f))))

(defn with-lmdb [f]
  (binding [*kv-backend* "crux.lmdb.LMDBKv"]
    (t/testing "LMDB"
      (f))))

(defn with-each-kv-store-implementation [f]
  (doseq [with-kv-store-implementation [with-memdb with-rocksdb with-lmdb]]
    (with-kv-store-implementation f)))

(def ^:dynamic *kafka-bootstrap-servers*)

(defn write-kafka-meta-properties [log-dir broker-id]
  (let [meta-properties (io/file log-dir "meta.properties")]
    (when-not (.exists meta-properties)
      (io/make-parents meta-properties)
      (with-open [out (io/output-stream meta-properties)]
        (doto (Properties.)
          (.setProperty "version" "0")
          (.setProperty "broker.id" (str broker-id))
          (.store out ""))))))

(def ^:dynamic ^AdminClient *admin-client*)

(defn with-embedded-kafka-cluster [f]
  (let [zookeeper-data-dir (cio/create-tmpdir "zookeeper")
        zookeeper-port (cio/free-port)
        kafka-log-dir (doto (cio/create-tmpdir "kafka-log")
                        (write-kafka-meta-properties ek/*broker-id*))
        kafka-port (cio/free-port)]
    (try
      (with-open [embedded-kafka (ek/start-embedded-kafka {:zookeeper-data-dir (str zookeeper-data-dir)
                                                           :zookeeper-port zookeeper-port
                                                           :kafka-log-dir (str kafka-log-dir)
                                                           :kafka-port kafka-port})
                  admin-client (k/create-admin-client
                                {"bootstrap.servers" (:bootstrap-servers embedded-kafka)})]
        (binding [*admin-client* admin-client
                  *kafka-bootstrap-servers* (:bootstrap-servers embedded-kafka)]
          (f)))
      (finally
        (cio/delete-dir kafka-log-dir)
        (cio/delete-dir zookeeper-data-dir)))))

(def ^:dynamic ^KafkaProducer *producer*)
(def ^:dynamic ^KafkaConsumer *consumer*)

(defn with-kafka-client [f]
  (with-open [producer (k/create-producer {"bootstrap.servers" *kafka-bootstrap-servers*})
              consumer (k/create-consumer {"bootstrap.servers" *kafka-bootstrap-servers*
                                           "group.id" "0"})]
    (binding [*producer* producer
              *consumer* consumer]
      (f))))

(def ^:dynamic *api-url*)
(def ^:dynamic *api*)

(defn with-local-node [f]
  (assert (bound? #'*kafka-bootstrap-servers*))
  (let [server-port (cio/free-port)
        db-dir (str (cio/create-tmpdir "kv-store"))
        test-id (UUID/randomUUID)
        tx-topic (str "tx-topic-" test-id)
        doc-topic (str "doc-topic-" test-id)]
    (try
      (with-open [local-node (api/start-local-node {:server-port server-port
                                                    :db-dir db-dir
                                                    :tx-topic tx-topic
                                                    :doc-topic doc-topic
                                                    :http-server? true
                                                    :kv-backend *kv-backend*
                                                    :bootstrap-servers *kafka-bootstrap-servers*})]
        (binding [*api* local-node
                  *api-url* (str "http://" ek/*host* ":" server-port)]
          (f)))
      (finally
        (cio/delete-dir db-dir)))))

(defn with-api-client [f]
  (assert (bound? #'*api-url*))
  (with-open [api-client (api/new-api-client *api-url*)]
    (binding [*api* api-client]
      (f))))

(defn with-each-api-implementation [f]
  (t/testing "Local API"
    (with-local-node f))
  (t/testing "Remote API"
    (with-local-node
      #(with-api-client f))))
