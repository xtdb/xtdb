(ns crux.fixtures
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [clojure.test.check.clojure-test :as tcct]
            [crux.api :as api]
            [crux.bootstrap :as b]
            [crux.codec :as c]
            [crux.db :as db]
            [crux.fixtures.kafka :refer [*kafka-bootstrap-servers*]]
            [crux.fixtures.kv :refer [*kv* *kv-backend*]]
            [crux.http-server :as srv]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.kafka.embedded :as ek]
            [crux.kv :as kv]
            [crux.lru :as lru]
            [crux.memory :as mem]
            [crux.tx :as tx]
            [taoensso.nippy :as nippy])
  (:import [crux.api Crux ICruxAPI]
           java.io.Closeable
           java.util.UUID))

(defn kv-object-store-w-cache [kv]
  (lru/->CachedObjectStore
    (lru/new-cache (:doc-cache-size b/default-options))
    (b/start-object-store {:kv kv} b/default-options)))

(defrecord KvTxLog [kv object-store]
  db/TxLog
  (submit-doc [this content-hash doc]
    (db/index-doc (tx/->KvIndexer kv this object-store) content-hash doc))

  (submit-tx [this tx-ops]
    (s/assert :crux.api/tx-ops tx-ops)
    (let [transact-time (cio/next-monotonic-date)
          tx-id (.getTime transact-time)
          tx-events (tx/tx-ops->tx-events tx-ops)
          indexer (tx/->KvIndexer kv this object-store)]
      (kv/store kv [[(c/encode-tx-log-key-to nil tx-id transact-time)
                     (nippy/fast-freeze tx-events)]])
      (doseq [doc (tx/tx-ops->docs tx-ops)]
        (db/submit-doc this (str (c/new-id doc)) doc))
      (db/index-tx indexer tx-events transact-time tx-id)
      (db/store-index-meta indexer
                           :crux.tx-log/consumer-state
                           {:crux.kv.topic-partition/tx-log-0
                            {:lag 0
                             :time transact-time}})
      (delay {:crux.tx/tx-id tx-id
              :crux.tx/tx-time transact-time})))

  (new-tx-log-context [this]
    (kv/new-snapshot kv))

  (tx-log [this tx-log-context from-tx-id]
    (let [i (kv/new-iterator tx-log-context)]
      (for [[k v] (idx/all-keys-in-prefix i (c/encode-tx-log-key-to nil from-tx-id) (c/encode-tx-log-key-to nil) true)]
        (assoc (c/decode-tx-log-key-from k)
               :crux.api/tx-ops (nippy/fast-thaw (mem/->on-heap v))))))

  Closeable
  (close [_]))

(defn kv-tx-log
  ([kv]
   (kv-tx-log kv (idx/->KvObjectStore kv)))
  ([kv object-store]
   (let [tx-log (->KvTxLog kv object-store)
         indexer (tx/->KvIndexer kv tx-log object-store)]
     (when-not (db/read-index-meta indexer :crux.tx-log/consumer-state)
       (db/store-index-meta
        indexer
        :crux.tx-log/consumer-state
        {:crux.kv.topic-partition/tx-log-0
         {:lag 0 :time nil}}))
     tx-log)))

(def ^:dynamic *api-url*)
(def ^:dynamic ^ICruxAPI *api*)
(def ^:dynamic ^ICruxAPI *cluster-node*)

(defn with-cluster-node [f]
  (assert (bound? #'*kafka-bootstrap-servers*))
  (assert (not (bound? #'*kv*)))
  (let [server-port (cio/free-port)
        db-dir (str (cio/create-tmpdir "kv-store"))
        test-id (UUID/randomUUID)
        tx-topic (str "tx-topic-" test-id)
        doc-topic (str "doc-topic-" test-id)
        options {:server-port server-port
                 :db-dir db-dir
                 :tx-topic tx-topic
                 :doc-topic doc-topic
                 :kv-backend *kv-backend*
                 :bootstrap-servers *kafka-bootstrap-servers*}]
    (try
      (with-open [cluster-node (Crux/startClusterNode options)
                  http-server (srv/start-http-server cluster-node options)]
        (binding [*cluster-node* cluster-node
                  *api* cluster-node
                  *api-url* (str "http://" ek/*host* ":" server-port)]
          (f)))
      (finally
        (cio/delete-dir db-dir)))))

(defn with-standalone-system [f]
  (assert (not (bound? #'*kv*)))
  (let [db-dir (str (cio/create-tmpdir "kv-store"))
        event-log-dir (str (cio/create-tmpdir "event-log-dir"))
        options {:db-dir db-dir
                 :event-log-dir event-log-dir
                 :kv-backend *kv-backend*}]
    (try
      (with-open [standalone-system (Crux/startStandaloneSystem options)]
        (binding [*api* standalone-system]
          (f)))
      (finally
        (cio/delete-dir db-dir)
        (cio/delete-dir event-log-dir)))))

(defn with-api-client [f]
  (assert (bound? #'*api-url*))
  (with-open [api-client (Crux/newApiClient *api-url*)]
    (binding [*api* api-client]
      (f))))

(defn with-each-api-implementation [f]
  (t/testing "Local API ClusterNode"
    (with-cluster-node f))
  (t/testing "Local API StandaloneSystem"
    (with-standalone-system f))
  (t/testing "Remote API"
    (with-cluster-node
      #(with-api-client f))))

(defn with-silent-test-check [f]
  (binding [tcct/*report-completion* false]
    (f)))

(defn maps->tx-ops
  ([maps]
   (vec (for [m maps]
          [:crux.tx/put m])))
  ([maps ts]
   (vec (for [m maps]
          [:crux.tx/put m ts]))))

(defn transact!
  "Helper fn for transacting entities"
  ([api entities]
   (transact! api entities (cio/next-monotonic-date)))
  ([^ICruxAPI api entities ts]
   (let [submitted-tx (api/submit-tx api (maps->tx-ops entities ts))]
     (api/sync api (:crux.tx/tx-time submitted-tx) nil))
   entities))

(defn entities->delete-tx-ops [entities ts]
  (vec (for [e entities]
         [:crux.tx/delete e ts])))

(defn delete-entities!
  ([api entities]
   (delete-entities! api entities (cio/next-monotonic-date)))
  ([api entities ts]
   (let [submitted-tx (api/submit-tx api (entities->delete-tx-ops entities ts))]
     (api/sync api (:crux.tx/tx-time submitted-tx) nil))
   entities))

(defn random-person [] {:crux.db/id (UUID/randomUUID)
                        :name      (rand-nth ["Ivan" "Petr" "Sergei" "Oleg" "Yuri" "Dmitry" "Fedor" "Denis"])
                        :last-name (rand-nth ["Ivanov" "Petrov" "Sidorov" "Kovalev" "Kuznetsov" "Voronoi"])
                        :sex       (rand-nth [:male :female])
                        :age       (rand-int 100)
                        :salary    (rand-int 100000)})

(defn people [people-mixins]
  (->> people-mixins (map merge (repeatedly random-person))))
