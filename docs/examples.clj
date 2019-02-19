(ns examples)

;; tag::start-system[]
(require '[crux.api :as api])

(def ^crux.api.ICruxSystem system (api/start-standalone-system {:kv-backend "crux.kv.memdb.MemKv"
                                                                :db-dir "data/db-dir-1"}))
;; end::start-system[]

;; tag::close-system[]
(.close system)
;; end::close-system[]

;; tag::start-local-node-system[]
(def ^crux.api.ICruxSystem system (api/start-local-node {:kv-backend "crux.kv.memdb.MemKv"
                                                         :bootstrap-servers "localhost:29092"}))
;; end::start-local-node-system[]

;; tag::start-standalone-with-rocks[]
(def ^crux.api.ICruxSystem system (api/start-standalone-system {:kv-backend "crux.kv.rocksdb.RocksKv"
                                                                :db-dir "data/db-dir-1"}))
;; end::start-standalone-with-rocks[]

;; tag::submit-tx[]
(api/submit-tx
 system
 [[:crux.tx/put :http://dbpedia.org/resource/Pablo_Picasso
   {:crux.db/id :http://dbpedia.org/resource/Pablo_Picasso
    :name "Pablo"
    :last-name "Picasso"}
   #inst "2018-05-18T09:20:27.966-00:00"]])
;; end::submit-tx[]

;; tag::query[]
(api/q (api/db system)
       '{:find [e]
         :where [[e :name "Pablo"]]})
;; end::query[]

(comment
  ;; tag::should-get[]
  #{[:http://dbpedia.org/resource/Pablo_Picasso]}
  ;; end::should-get[]
  )

;; tag::ek-example[]
(require '[crux.kafka.embedded :as ek])

(def storage-dir "dev-storage")
(def embedded-kafka-options
  {:crux.kafka.embedded/zookeeper-data-dir (str storage-dir "/zookeeper")
   :crux.kafka.embedded/kafka-log-dir (str storage-dir "/kafka-log")
   :crux.kafka.embedded/kafka-port 9092})

(def embedded-kafka (ek/start-embedded-kafka embedded-kafka-options))
;; end::ek-example[]

;; tag::ek-close[]
(.close embedded-kafka)
;; end::ek-close[]

;; tag::http-setup[]
(require '[crux.http-server :as srv])

(def http-server-options {:server-port 3000})
(def http-server (srv/start-http-server local-node http-server-options))
;; end::http-setup[]

;; tag::http-close[]
(.close http-server)
;; end::http-close[]
