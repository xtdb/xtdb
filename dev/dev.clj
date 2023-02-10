(ns dev
  "Internal development namespace for XTDB. For end-user usage, see
  examples.clj"
  (:require [clojure.java.io :as io]
            [integrant.core :as i]
            [integrant.repl :as ir :refer :all]
            [integrant.repl.state :refer [system]]
            [xtdb.api :as xt]
            [xtdb.fixtures.tpch :as tpch]
            [xtdb.io :as xio]
            [xtdb.kafka :as k]
            [xtdb.kafka.embedded :as ek]
            [xtdb.lucene]
            [xtdb.checkpoint]
            [xtdb.rocksdb :as rocks])
  (:import (ch.qos.logback.classic Level Logger)
           (java.io Closeable File)
           (org.slf4j LoggerFactory)
           (xtdb.api IXtdb)))

(defn set-log-level! [ns level]
  (.setLevel ^Logger (LoggerFactory/getLogger (name ns))
             (when level
               (Level/valueOf (name level)))))

(defn get-log-level! [ns]
  (some->> (.getLevel ^Logger (LoggerFactory/getLogger (name ns)))
           (str)
           (.toLowerCase)
           (keyword)))

(defmacro with-log-level [ns level & body]
  `(let [level# (get-log-level! ~ns)]
     (try
       (set-log-level! ~ns ~level)
       ~@body
       (finally
         (set-log-level! ~ns level#)))))

(def dev-node-dir
  (io/file "dev/dev-node"))

(defn- delay-funcall
  "Delay function call `ms` milliseconds"
  [ms f & args]
  (when (pos? ms)
    (Thread/sleep ms))
  (apply f args))

(defmethod i/init-key ::xtdb [_ {:keys [node-opts]}]
  (xt/start-node node-opts))

;; simulate a long-lasting recovery.
(defmethod i/init-key ::xtdb* [_ {:keys [node-opts recovery-delay]}]
  (let [sync-path @#'xtdb.checkpoint/sync-path]
    (with-redefs [xtdb.checkpoint/sync-path (partial delay-funcall recovery-delay sync-path)]
      (xt/start-node node-opts))))

(defmethod i/halt-key! ::xtdb [_ ^IXtdb node]
  (.close node))

;; this config starts the HTTP health-check server and set
;; (filesystem) checkpointing. Upon starting, if
;;   * the XT node detects it needs a recovery
;;     (e.g., `dev/dev-node-checkpoint` is missing), and
;;   * some checkpoint is available, then
;; XT will start a recovery pausing for `recovery-delay`.
(def checkpoint-fs-healthz-config
  {::xtdb*
   {:recovery-delay 10000 ;; milliseconds
    :node-opts
    {:xtdb.http-health-check/server {:port 8080}
     :xtdb/index-store
     {:kv-store
      {:xtdb/module `rocks/->kv-store,
       :db-dir (io/file dev-node-dir "indexes"),
       :checkpointer
       {:xtdb/module 'xtdb.checkpoint/->checkpointer
        :store {:xtdb/module 'xtdb.checkpoint/->filesystem-checkpoint-store
                :path (io/file dev-node-dir "checkpoints")}
        :approx-frequency (java.time.Duration/ofSeconds 30)}}}

     :xtdb/document-store
     {:kv-store {:xtdb/module `rocks/->kv-store,
                 :db-dir (io/file dev-node-dir "documents")
                 :block-cache :xtdb.rocksdb/block-cache}}

     :xtdb/tx-log
     {:kv-store {:xtdb/module `rocks/->kv-store,
                 :db-dir (io/file dev-node-dir "tx-log")
                 :block-cache :xtdb.rocksdb/block-cache}}

     :xtdb.rocksdb/block-cache
     {:xtdb/module `rocks/->lru-block-cache
      :cache-size (* 128 1024 1024)}}}})

(def checkpoint-fs-config
  {::xtdb
   {:node-opts
    {:xtdb/index-store
     {:kv-store
      {:xtdb/module `rocks/->kv-store,
       :db-dir (io/file dev-node-dir "indexes"),
       :checkpointer
       {:xtdb/module 'xtdb.checkpoint/->checkpointer
        :store {:xtdb/module 'xtdb.checkpoint/->filesystem-checkpoint-store
                :path (io/file dev-node-dir "checkpoints")}
        :approx-frequency (java.time.Duration/ofSeconds 30)}}}

     :xtdb/document-store
     {:kv-store {:xtdb/module `rocks/->kv-store,
                 :db-dir (io/file dev-node-dir "documents")
                 :block-cache :xtdb.rocksdb/block-cache}}

     :xtdb/tx-log
     {:kv-store {:xtdb/module `rocks/->kv-store,
                 :db-dir (io/file dev-node-dir "tx-log")
                 :block-cache :xtdb.rocksdb/block-cache}}

     :xtdb.rocksdb/block-cache
     {:xtdb/module `rocks/->lru-block-cache
      :cache-size (* 128 1024 1024)}}}})


(def standalone-config
  {::xtdb {:node-opts {:xtdb/index-store {:kv-store {:xtdb/module `rocks/->kv-store,
                                                     :db-dir (io/file dev-node-dir "indexes"),
                                                     :block-cache :xtdb.rocksdb/block-cache}}
                       :xtdb/document-store {:kv-store {:xtdb/module `rocks/->kv-store,
                                                        :db-dir (io/file dev-node-dir "documents")
                                                        :block-cache :xtdb.rocksdb/block-cache}}
                       :xtdb/tx-log {:kv-store {:xtdb/module `rocks/->kv-store,
                                                :db-dir (io/file dev-node-dir "tx-log")
                                                :block-cache :xtdb.rocksdb/block-cache}}
                       :xtdb.rocksdb/block-cache {:xtdb/module `rocks/->lru-block-cache
                                                  :cache-size (* 128 1024 1024)}
                       :xtdb.metrics.jmx/reporter {}
                       :xtdb.http-server/server {}
                       :xtdb.lucene/lucene-store {:db-dir (io/file dev-node-dir "lucene")}}}})

(defmethod i/init-key ::embedded-kafka [_ {:keys [kafka-port kafka-dir]}]
  {:embedded-kafka (ek/start-embedded-kafka #::ek{:zookeeper-data-dir (io/file kafka-dir "zk-data")
                                                  :zookeeper-port (xio/free-port)
                                                  :kafka-log-dir (io/file kafka-dir "kafka-log")
                                                  :kafka-port kafka-port})
   :meta-properties-file (io/file kafka-dir "kafka-log/meta.properties")})

(defmethod i/halt-key! ::embedded-kafka [_ {:keys [^Closeable embedded-kafka ^File meta-properties-file]}]
  (.close embedded-kafka)
  (.delete meta-properties-file))

(def embedded-kafka-config
  (let [kafka-port (xio/free-port)]
    {::embedded-kafka {:kafka-port kafka-port
                       :kafka-dir (io/file dev-node-dir (str "kafka." kafka-port))}
     ::xtdb {:ek (i/ref ::embedded-kafka)
             :node-opts {::k/kafka-config {:bootstrap-servers (str "http://localhost:" kafka-port)}
                         :xtdb/index-store {:kv-store {:xtdb/module `rocks/->kv-store
                                                       :db-dir (io/file dev-node-dir "ek-indexes")}}
                         :xtdb/document-store {:xtdb/module `k/->document-store,
                                               :kafka-config ::k/kafka-config
                                               :local-document-store {:kv-store {:xtdb/module `rocks/->kv-store,
                                                                                 :db-dir (io/file dev-node-dir "ek-documents")}}}
                         :xtdb/tx-log {:xtdb/module `k/->tx-log,
                                       :kafka-config ::k/kafka-config
                                       #_#_:poll-wait-duration (java.time.Duration/ofMillis 10)}}}}))

;; this config assumes you have access to a Kafka broker.
(def local-kafka-config
  {::xtdb {:node-opts
           {:kafka-config {:xtdb/module 'xtdb.kafka/->kafka-config
                           :bootstrap-servers "localhost:9092"}

            :xtdb/index-store {:kv-store {:xtdb/module `rocks/->kv-store
                                          :db-dir (io/file dev-node-dir "xtdb-indexes")}}

            :xtdb/tx-log {:xtdb/module 'xtdb.kafka/->tx-log
                          :tx-topic-opts {:topic-name "xtdb-transaction-log"}
                          :kafka-config :kafka-config
                          #_#_:poll-wait-duration (java.time.Duration/ofMillis 10)}

            :xtdb/document-store {:xtdb/module 'xtdb.kafka/->document-store
                                  :doc-topic-opts {:topic-name "xtdb-document-store"}
                                  :kafka-config :kafka-config}}}})


;; swap for `embedded-kafka-config`  to use embedded-kafka
(ir/set-prep! (fn [] standalone-config))
; (ir/set-prep! (fn [] local-kafka-config))
; (ir/set-prep! (fn [] embedded-kafka-config))
; (ir/set-prep! (fn [] checkpoint-fs-config))
; (ir/set-prep! (fn [] checkpoint-fs-healthz-config))


(defn xtdb-node []
  (::xtdb system))

(comment
  (tpch/load-docs! (dev/xtdb-node) 0.05)

  (time
   (count
    (tpch/run-query (xt/db (xtdb-node))
                    (-> tpch/q5
                        (assoc :timeout 120000)))))

  (time
   (doseq [q tpch/tpch-queries]
     (time
      (tpch/run-query (xt/db (xtdb-node))
                      (-> q
                          (assoc :timeout 120000)))))))
