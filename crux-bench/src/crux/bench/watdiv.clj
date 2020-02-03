(ns crux.bench.watdiv
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [crux.bench :as bench]
            [crux.api :as crux]
            [crux.index :as idx]
            [crux.rdf :as rdf]
            [crux.sparql :as sparql]
            [datomic.api :as d]
            [crux.io :as cio])
  (:import [java.io Closeable File]
           java.time.Duration
           java.util.Date))

(def supported-backends
  [:crux])

(defmulti start-watdiv-runner
  (fn [key node] key))

(def query-timeout-ms 15000)

(defprotocol WatdivBackend
  (backend-info [this])
  (execute-with-timeout [this datalog])
  (ingest-watdiv-data [this resource]))

(defn entity->idents [e]
  (cons
   {:db/ident (:crux.db/id e)}
   (for [[_ v] e
         v (idx/vectorize-value v)
         :when (keyword? v)]
     {:db/ident v})))

(defn entity->datomic [e]
  (let [id (:crux.db/id e)
        tx-op-fn (fn tx-op-fn [k v]
                   (if (set? v)
                     (vec (mapcat #(tx-op-fn k %) v))
                     [[:db/add id k v]]))]
    (->> (for [[k v] (dissoc e :crux.db/id)]
           (tx-op-fn k v))
         (apply concat)
         (vec))))

(def datomic-tx-size 100)

(defn load-rdf-into-datomic [conn resource]
  (with-open [in (io/input-stream (io/resource resource))]
    (->> (rdf/ntriples-seq in)
         (rdf/statements->maps)
         (map #(rdf/use-default-language % rdf/*default-language*))
         (partition-all datomic-tx-size)
         (reduce (fn [^long n entities]
                   (let [done? (atom false)]
                     (while (not @done?)
                       (try
                         (when (zero? (long (mod n rdf/*ntriples-log-size*)))
                           (log/debug "submitted" n))
                         @(d/transact conn (mapcat entity->idents entities))
                         @(d/transact conn (->> (map entity->datomic entities)
                                                (apply concat)
                                                (vec)))
                         (reset! done? true)
                         (catch Exception e
                           (println (ex-data e))
                           (println (ex-data (.getCause e)))
                           (println "retry again to submit!")
                           (Thread/sleep 10000))))
                     (+ n (count entities))))
                 0))))

(defrecord DatomicBackend [conn]
  WatdivBackend
  (backend-info [this]
    {:backend :datomic})
  (execute-with-timeout [this datalog]
    (d/query {:query datalog
              :timeout query-timeout-ms
              :args [(d/db conn)]}))
  (ingest-watdiv-data [this resource]
    (when-not (d/entity (d/db conn) [:watdiv/ingest-state :global])
      (log/info "starting to ingest watdiv data into datomic")
      (let [time-before (Date.)]
        (load-rdf-into-datomic conn resource)
        (let [ingest-time (- (.getTime (Date.)) (.getTime time-before))]
          (log/infof "completed datomic watdiv ingestion time taken: %s" ingest-time)
          @(d/transact conn [{:watdiv/ingest-state :global
                              :watdiv/ingest-time ingest-time}]))))))

;; See: https://dsg.uwaterloo.ca/watdiv/watdiv-data-model.txt
;; Some things like dates are strings in the actual data.
(def datomic-watdiv-schema
  [#:db{:ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/composer")
        :cardinality :db.cardinality/one
        :valueType :db.type/string}
   #:db{:ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/follows")
        :cardinality :db.cardinality/many
        :valueType :db.type/ref}
   #:db{:ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/friendOf")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/gender")
        :valueType :db.type/ref
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/hits")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/likes")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/makesPurchase")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/purchaseDate")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor")
        :valueType :db.type/ref
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/subscribes")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/userId")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://ogp.me/ns#tag")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://ogp.me/ns#title")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/dc/terms/Location")
        :valueType :db.type/ref
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/goodrelations/description")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/goodrelations/includes")
        :valueType :db.type/ref
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/goodrelations/name")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/goodrelations/offers")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://purl.org/goodrelations/price")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/goodrelations/serialNumber")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/goodrelations/validFrom")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/goodrelations/validThrough")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/ontology/mo/artist")
        :valueType :db.type/ref
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/ontology/mo/conductor")
        :valueType :db.type/ref
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/ontology/mo/movement")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/ontology/mo/opus")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/ontology/mo/performed_in")
        :valueType :db.type/ref
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/ontology/mo/performer")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/ontology/mo/producer")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/ontology/mo/record_number")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/ontology/mo/release")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/stuff/rev#hasReview")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://purl.org/stuff/rev#rating")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/stuff/rev#reviewer")
        :valueType :db.type/ref
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/stuff/rev#text")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/stuff/rev#title")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://purl.org/stuff/rev#totalVotes")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/actor")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://schema.org/aggregateRating")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/author")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://schema.org/award")
        :valueType :db.type/string
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://schema.org/birthDate")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/bookEdition")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/caption")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/contactPoint")
        :valueType :db.type/ref
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/contentRating")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/contentSize")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/datePublished")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/description")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/director")
        :valueType :db.type/ref
        :cardinality :db.cardinality/one}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/duration")}
   #:db{:ident (keyword "http://schema.org/editor")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://schema.org/eligibleQuantity")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/eligibleRegion")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://schema.org/email")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/employee")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://schema.org/expires")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/faxNumber")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/isbn")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/jobTitle")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/keywords")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/language")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://schema.org/legalName")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/nationality")
        :valueType :db.type/ref
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/numberOfPages")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/openingHours")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/paymentAccepted")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/priceValidUntil")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/printColumn")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/printEdition")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/printPage")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/printSection")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/producer")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/publisher")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/telephone")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/text")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/trailer")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://schema.org/url")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://schema.org/wordCount")
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://www.geonames.org/ontology#parentCountry")
        :valueType :db.type/ref
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
        :valueType :db.type/ref
        :cardinality :db.cardinality/many}
   #:db{:ident (keyword "http://xmlns.com/foaf/age")
        :valueType :db.type/ref
        :cardinality :db.cardinality/one}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://xmlns.com/foaf/familyName")}
   #:db{:ident (keyword "http://xmlns.com/foaf/givenName")
        :valueType :db.type/string
        :cardinality :db.cardinality/one}
   #:db{:ident (keyword "http://xmlns.com/foaf/homepage")
        :valueType :db.type/ref
        :cardinality :db.cardinality/one}
   #:db{:ident :watdiv/ingest-time
        :valueType :db.type/long
        :cardinality :db.cardinality/one}
   #:db{:ident :watdiv/ingest-state
        :valueType :db.type/keyword
        :cardinality :db.cardinality/one
        :unique :db.unique/identity
        :index true}])

(defmethod start-watdiv-runner :datomic
  [_ node]
  (let [uri (str "datomic:free://"
                 (or (System/getenv "DATOMIC_TRANSACTOR_URI") "datomic")
                 ":4334/bench?password=password")
        _ (d/create-database uri)
        conn (d/connect uri)]
    @(d/transact conn datomic-watdiv-schema)
    (map->DatomicBackend {:conn conn})))

(defrecord WatdivRunner [running-future]
  Closeable
  (close [_]
    (future-cancel running-future)))

(def watdiv-tests
  {"watdiv-stress-100/warmup.1.desc" "watdiv-stress-100/warmup.sparql"
   "watdiv-stress-100/test.1.desc" "watdiv-stress-100/test.1.sparql"
   "watdiv-stress-100/test.2.desc" "watdiv-stress-100/test.2.sparql"
   "watdiv-stress-100/test.3.desc" "watdiv-stress-100/test.3.sparql"
   "watdiv-stress-100/test.4.desc" "watdiv-stress-100/test.4.sparql"})

;; above is old code =====

(defn ingest-crux
  [node]
  (bench/run-bench :ingest
                   (let [{:keys [last-tx entity-count]}
                         (with-open [in (io/input-stream (io/resource "watdiv.10M.nt"))]
                           (rdf/submit-ntriples (:tx-log node) in 1000))]
                     (crux/await-tx node @last-tx)
                     {:entity-count entity-count})))

(defn execute-stress-test-crux
  [node num-tests ^Long num-threads]
  (bench/run-bench :stress-test
                   (let [all-jobs-submitted (atom false)
                         all-jobs-completed (atom false)
                         pool (java.util.concurrent.Executors/newFixedThreadPool (inc num-threads))
                         job-queue (java.util.concurrent.LinkedBlockingQueue. num-threads)
                         completed-queue (java.util.concurrent.LinkedBlockingQueue. ^Long (* 10 num-threads))
                         job-features (mapv (fn [_]
                                              (.submit
                                                pool
                                                ^Runnable
                                                (fn run-jobs []
                                                  (when-let [{:keys [q]} (if @all-jobs-submitted
                                                                           (.poll job-queue)
                                                                           (.take job-queue))]
                                                    (try
                                                      {:result-count
                                                       (log/error (crux/q
                                                                    (crux/db node)
                                                                    (sparql/sparql->datalog q)))}
                                                      (catch java.util.concurrent.TimeoutException t
                                                        {:error t}))
                                                    (recur)))))
                                            (range num-threads))]
                     (try
                       (with-open [desc-in (io/reader (io/resource "watdiv-stress-100/test.1.desc"))
                                   sparql-in (io/reader (io/resource "watdiv-stress-100/test.1.sparql"))]
                         (doseq [[idx [_ q]] (->> (map vector (line-seq desc-in) (line-seq sparql-in))
                                                  (take (or num-tests 100))
                                                  (map-indexed vector))]
                           (.put job-queue {:idx idx :q q})))
                       (reset! all-jobs-submitted true)
                       (doseq [^java.util.concurrent.Future f job-features] (.get f))
                       (reset! all-jobs-completed true)
                       (catch InterruptedException e
                         (.shutdownNow pool)
                         (throw e))))))

(defn run-watdiv-bench
  [node {:keys [test-count thread-count]}]
  (bench/with-bench-ns :watdiv
    (ingest-crux node)
    (execute-stress-test-crux node test-count thread-count)))
