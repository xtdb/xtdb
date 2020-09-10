(ns user
  (:require [crux.api :as crux]
            [clojure.tools.namespace.repl :as ctn]
            [integrant.core :as i]
            [integrant.repl.state :refer [system]]
            [integrant.repl :as ir :refer [clear go suspend resume halt reset reset-all]]
            [crux.io :as cio]
            [crux.kafka :as k]
            [crux.kafka.embedded :as ek]
            [crux.rocksdb :as rocks]
            [clojure.java.io :as io]
            [crux.system :as sys])
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

(defmethod i/init-key :crux [_ node-opts]
  (crux/start-node node-opts))

(defmethod i/halt-key! :crux [_ ^ICruxAPI node]
  (.close node))

(def standalone-config
  {:crux {:crux/indexer {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file dev-node-dir "indexes")}}
          :crux/document-store {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file dev-node-dir "documents")}}
          :crux/tx-log {:kv-store {:crux/module `rocks/->kv-store, :db-dir (io/file dev-node-dir "tx-log")}}
          :crux.metrics.jmx/reporter {}
          :crux.http-server/server {}}})

(defmethod i/init-key :embedded-kafka [_ {:keys [kafka-port kafka-dir]}]
  (ek/start-embedded-kafka #::ek{:zookeeper-data-dir (io/file kafka-dir "zk-data")
                                 :zookeeper-port (cio/free-port)
                                 :kafka-log-dir (io/file kafka-dir "kafka-log")
                                 :kafka-port kafka-port}))

(defmethod i/halt-key! :embedded-kafka [_ ^Closeable embedded-kafka]
  (.close embedded-kafka))

(def embedded-kafka-config
  (let [kafka-port (cio/free-port)]
    {:embedded-kafka {:kafka-port kafka-port
                      :kafka-dir (io/file dev-node-dir "kafka")}
     :crux {::k/kafka-config {:bootstrap-servers (str "localhost:" kafka-port)}
            :crux/indexer {:kv-store {:crux/module `rocks/->kv-store
                                      :db-dir (io/file dev-node-dir "ek-indexes")}}
            :crux/document-store {:crux/module `k/->document-store,
                                  :kafka-config ::k/kafka-config
                                  :local-document-store {:kv-store {:crux/module `rocks/->kv-store,
                                                                    :db-dir (io/file dev-node-dir "ek-documents")}}}
            :crux/tx-log {:crux/module `k/->tx-log, :kafka-config ::k/kafka-config}}}))

;; swap for `embedded-kafka-config` to use embedded-kafka
(ir/set-prep! (fn [] standalone-config))

(defn crux-node []
  (:crux system))
