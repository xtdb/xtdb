(ns xtdb.docs.examples-test
  (:require [clojure.java.io :as io]
            [xtdb.api :as xt]))

;; tag::require-ek[]
(require '[xtdb.kafka.embedded :as ek])
;; end::require-ek[]

;; tag::ek-example[]
(defn start-embedded-kafka [kafka-port storage-dir]
  (ek/start-embedded-kafka {:xtdb.kafka.embedded/zookeeper-data-dir (io/file storage-dir "zk-data")
                            :xtdb.kafka.embedded/kafka-log-dir (io/file storage-dir "kafka-log")
                            :xtdb.kafka.embedded/kafka-port kafka-port}))
;; end::ek-example[]

(defn stop-embedded-kafka [^java.io.Closeable embedded-kafka]
;; tag::ek-close[]
(.close embedded-kafka)
;; end::ek-close[]
)
