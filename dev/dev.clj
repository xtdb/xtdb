(ns dev
  "Internal development namespace for Crux. For end-user usage, see
  examples.clj"
  (:require [xtdb.api :as xt]
            [integrant.core :as i]
            [integrant.repl.state :refer [system]]
            [integrant.repl :as ir :refer [go halt reset reset-all]]
            [xtdb.io :as xio]
            [xtdb.lucene]
            [xtdb.kafka :as k]
            [xtdb.kafka.embedded :as ek]
            [xtdb.rocksdb :as rocks]
            [clojure.java.io :as io])
  (:import (xtdb.api IXtdb)
           java.io.Closeable
           [ch.qos.logback.classic Level Logger]
           org.slf4j.LoggerFactory))

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

(defmethod i/init-key ::crux [_ {:keys [node-opts]}]
  (xt/start-node node-opts))

(defmethod i/halt-key! ::crux [_ ^IXtdb node]
  (.close node))

(def standalone-config
  {::crux {:node-opts {:xt/index-store {:kv-store {:xt/module `rocks/->kv-store,
                                                   :db-dir (io/file dev-node-dir "indexes"),
                                                   :block-cache :xtdb.rocksdb/block-cache}}
                       :xt/document-store {:kv-store {:xt/module `rocks/->kv-store,
                                                      :db-dir (io/file dev-node-dir "documents")
                                                      :block-cache :xtdb.rocksdb/block-cache}}
                       :xt/tx-log {:kv-store {:xt/module `rocks/->kv-store,
                                              :db-dir (io/file dev-node-dir "tx-log")
                                              :block-cache :xtdb.rocksdb/block-cache}}
                       :xtdb.rocksdb/block-cache {:xt/module `rocks/->lru-block-cache
                                                  :cache-size (* 128 1024 1024)}
                       :xtdb.metrics.jmx/reporter {}
                       :xtdb.http-server/server {}
                       :xtdb.lucene/lucene-store {:db-dir (io/file dev-node-dir "lucene")}}}})

(defmethod i/init-key ::embedded-kafka [_ {:keys [kafka-port kafka-dir]}]
  (ek/start-embedded-kafka #::ek{:zookeeper-data-dir (io/file kafka-dir "zk-data")
                                 :zookeeper-port (xio/free-port)
                                 :kafka-log-dir (io/file kafka-dir "kafka-log")
                                 :kafka-port kafka-port}))

(defmethod i/halt-key! ::embedded-kafka [_ ^Closeable embedded-kafka]
  (.close embedded-kafka))

(def embedded-kafka-config
  (let [kafka-port (xio/free-port)]
    {::embedded-kafka {:kafka-port kafka-port
                       :kafka-dir (io/file dev-node-dir "kafka")}
     ::crux {:ek (i/ref ::embedded-kafka)
             :node-opts {::k/kafka-config {:bootstrap-servers (str "http://localhost:" kafka-port)}
                         :xt/index-store {:kv-store {:xt/module `rocks/->kv-store
                                                     :db-dir (io/file dev-node-dir "ek-indexes")}}
                         :xt/document-store {:xt/module `k/->document-store,
                                             :kafka-config ::k/kafka-config
                                             :local-document-store {:kv-store {:xt/module `rocks/->kv-store,
                                                                               :db-dir (io/file dev-node-dir "ek-documents")}}}
                         :xt/tx-log {:xt/module `k/->tx-log, :kafka-config ::k/kafka-config}}}}))

;; swap for `embedded-kafka-config` to use embedded-kafka
(ir/set-prep! (fn [] standalone-config))

(defn crux-node []
  (::crux system))
