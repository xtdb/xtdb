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

(defmethod i/init-key ::xtdb [_ {:keys [node-opts]}]
  (xt/start-node node-opts))

(defmethod i/halt-key! ::xtdb [_ ^IXtdb node]
  (.close node))

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
                       :kafka-dir (io/file dev-node-dir "kafka")}
     ::xtdb {:ek (i/ref ::embedded-kafka)
             :node-opts {::k/kafka-config {:bootstrap-servers (str "http://localhost:" kafka-port)}
                         :xtdb/index-store {:kv-store {:xtdb/module `rocks/->kv-store
                                                       :db-dir (io/file dev-node-dir "ek-indexes")}}
                         :xtdb/document-store {:xtdb/module `k/->document-store,
                                               :kafka-config ::k/kafka-config
                                               :local-document-store {:kv-store {:xtdb/module `rocks/->kv-store,
                                                                                 :db-dir (io/file dev-node-dir "ek-documents")}}}
                         :xtdb/tx-log {:xtdb/module `k/->tx-log, :kafka-config ::k/kafka-config}}}}))

;; swap for `embedded-kafka-config` to use embedded-kafka
(ir/set-prep! (fn [] standalone-config))

(defn xtdb-node []
  (::xtdb system))

(comment
  (time
   (xt/q (xt/db (xtdb-node))
         (-> tpch/q14
             (assoc :timeout 120000)))))
