(ns user
  (:require [crux.api :as crux]
            [clojure.tools.namespace.repl :as ctn]
            [integrant.core :as i]
            [integrant.repl.state :refer [system]]
            [integrant.repl :as ir :refer [clear go suspend resume halt reset reset-all]]
            [crux.io :as cio]
            [crux.kafka.embedded :as ek]
            [clojure.java.io :as io])
  (:import (crux.api ICruxAPI)
           (java.io Closeable File)))

(ctn/disable-reload!)
(apply ctn/set-refresh-dirs (for [^File dir (.listFiles (io/file "."))
                                  :when (and (.isDirectory dir)
                                             (not (-> dir .getName #{"crux-graal"})))
                                  sub-dir #{"src" "test"}]
                              (io/file dir sub-dir)))

(def dev-node-dir
  (io/file "dev/dev-node"))

(defmethod i/init-key :node [_ node-opts]
  (crux/start-node node-opts))

(defmethod i/halt-key! :node [_ ^ICruxAPI node]
  (.close node))

(def standalone-config
  {:node {:crux.node/topology ['crux.standalone/topology
                               'crux.kv.rocksdb/kv-store]
          :crux.kv/db-dir (str (io/file dev-node-dir "db"))
          :crux.standalone/event-log-kv-store 'crux.kv.rocksdb/kv
          :crux.standalone/event-log-dir (str (io/file dev-node-dir "event-log"))
          :crux.kv/sync? true}})

(defmethod i/init-key :embedded-kafka [_ {:keys [kafka-port kafka-dir]}]
  (ek/start-embedded-kafka #::ek{:zookeeper-data-dir (str (io/file kafka-dir "zk-data"))
                                 :zookeeper-port (cio/free-port)
                                 :kafka-log-dir (str (io/file kafka-dir "kafka-log"))
                                 :kafka-port kafka-port}))

(defmethod i/halt-key! :embedded-kafka [_ ^Closeable embedded-kafka]
  (.close embedded-kafka))

(def embedded-kafka-config
  (let [kafka-port (cio/free-port)]
    {:embedded-kafka {:kafka-port kafka-port
                      :kafka-dir (io/file dev-node-dir "kafka")}
     :node {:crux.node/topology ['crux.kafka/topology
                                 'crux.kv.rocksdb/kv-store]
            :crux.kafka/bootstrap-servers (str "localhost:" kafka-port)
            :crux.kv/db-dir (str (io/file dev-node-dir "ek-db"))
            :crux.standalone/event-log-dir (str (io/file dev-node-dir "ek-event-log"))
            :crux.kv/sync? true}}))


;; swap for `embedded-kafka-config` to use embedded-kafka
(ir/set-prep! (fn [] standalone-config))

(defn crux-node []
  (:node system))
