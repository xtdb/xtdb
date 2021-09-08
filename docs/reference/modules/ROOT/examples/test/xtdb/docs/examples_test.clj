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

;; tag::start-http-client[]
(defn start-http-client [port]
  (xt/new-api-client (str "http://localhost:" port)))
;; end::start-http-client[]

(defn example-query-entity [node]
;; tag::query-entity[]
(xt/entity (xt/db node) :dbpedia.resource/Pablo-Picasso)
;; end::query-entity[]
)

(defn example-query-valid-time [node]
;; tag::query-valid-time[]
(xt/q (xt/db node #inst "2018-05-19T09:20:27.966-00:00")
        '{:find [e]
          :where [[e :name "Pablo"]]})
;; end::query-valid-time[]
)



#_(comment
;; tag::history-full[]
(xt/submit-tx
  node
  [[::xt/put
    {:xt/id :ids.persons/Jeff
     :person/name "Jeff"
     :person/wealth 100}
    #inst "2018-05-18T09:20:27.966"]
   [::xt/put
    {:xt/id :ids.persons/Jeff
     :person/name "Jeff"
     :person/wealth 1000}
    #inst "2015-05-18T09:20:27.966"]])

; yields
{::xt/tx-id 1555314836178,
 ::xt/tx-time #inst "2019-04-15T07:53:56.178-00:00"}

; Returning the history in descending order
; To return in ascending order, use :asc in place of :desc
(xt/entity-history (xt/db node) :ids.persons/Jeff :desc)

; yields
[{::xt/tx-time #inst "2019-04-15T07:53:55.817-00:00",
  ::xt/tx-id 1555314835817,
  ::xt/valid-time #inst "2018-05-18T09:20:27.966-00:00",
  ::xt/content-hash ; sha1 hash of document contents
  "6ca48d3bf05a16cd8d30e6b466f76d5cc281b561"}
 {::xt/tx-time #inst "2019-04-15T07:53:56.178-00:00",
  ::xt/tx-id 1555314836178,
  ::xt/valid-time #inst "2015-05-18T09:20:27.966-00:00",
  ::xt/content-hash "a95f149636e0a10a78452298e2135791c0203529"}]
;; end::history-full[]

;; tag::history-with-docs[]
(xt/entity-history (xt/db node) :ids.persons/Jeff :desc {:with-docs? true})

; yields
[{::xt/tx-time #inst "2019-04-15T07:53:55.817-00:00",
  ::xt/tx-id 1555314835817,
  ::xt/valid-time #inst "2018-05-18T09:20:27.966-00:00",
  ::xt/content-hash
  "6ca48d3bf05a16cd8d30e6b466f76d5cc281b561"
  ::xt/doc
  {:xt/id :ids.persons/Jeff
   :person/name "Jeff"
   :person/wealth 100}}
 {::xt/tx-time #inst "2019-04-15T07:53:56.178-00:00",
  ::xt/tx-id 1555314836178,
  ::xt/valid-time #inst "2015-05-18T09:20:27.966-00:00",
  ::xt/content-hash "a95f149636e0a10a78452298e2135791c0203529"
  ::xt/doc
  {:xt/id :ids.persons/Jeff
   :person/name "Jeff"
   :person/wealth 1000}}]
;; end::history-with-docs[]

;; tag::history-range[]

; Passing the additional 'opts' map with the start/end bounds.
; As we are returning results in :asc order, the map contains the earlier starting coordinates -
; If returning history range in descending order, we pass the later coordinates as start coordinates to the map
(xt/entity-history
 (xt/db node)
 :ids.persons/Jeff
 :asc
 {:start-valid-time #inst "2015-05-18T09:20:27.966"
  :start-tx-time #inst "2015-05-18T09:20:27.966"
  :end-valid-time #inst "2020-05-18T09:20:27.966"
  :end-tx-time #inst "2020-05-18T09:20:27.966"})

; yields
[{::xt/tx-time #inst "2019-04-15T07:53:56.178-00:00",
  ::xt/tx-id 1555314836178,
  ::xt/valid-time #inst "2015-05-18T09:20:27.966-00:00",
  ::xt/content-hash
  "a95f149636e0a10a78452298e2135791c0203529"}
 {::xt/tx-time #inst "2019-04-15T07:53:55.817-00:00",
  ::xt/tx-id 1555314835817
  ::xt/valid-time #inst "2018-05-18T09:20:27.966-00:00",
  ::xt/content-hash "6ca48d3bf05a16cd8d30e6b466f76d5cc281b561"}]

;; end::history-range[]
)
