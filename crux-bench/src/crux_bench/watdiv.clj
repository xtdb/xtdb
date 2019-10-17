(ns crux-bench.watdiv
  (:require [amazonica.aws.s3 :as s3]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [crux.api :as crux]
            [crux.index :as idx]
            [crux.rdf :as rdf]
            [crux.sparql :as sparql]
            [datomic.api :as d])
  (:import com.amazonaws.services.s3.model.CannedAccessControlList
           [java.io Closeable File]
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
         v (idx/normalize-value v)
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

(defrecord CruxBackend [crux]
  WatdivBackend
  (backend-info [this]
    (let [ingest-stats (crux/entity (crux/db crux) ::watdiv-ingestion-status)]
      (merge
        {:backend :crux}
        (select-keys ingest-stats [:watdiv/ingest-start-time
                                   :watdiv/kafka-ingest-time
                                   :watdiv/ingest-time])
        (select-keys (crux/status crux) [:crux.version/version
                                         :crux.version/revision
                                         :crux.kv/kv-store]))))

  (execute-with-timeout [this datalog]
    (let [db (crux/db crux)]
      (with-open [snapshot (crux/new-snapshot db)]
        (let [query-future (future (count (crux/q db snapshot datalog)))]
          (or (deref query-future query-timeout-ms nil)
              (do (future-cancel query-future)
                  (throw (IllegalStateException. "Query timed out."))))))))

  (ingest-watdiv-data [this resource]
    (when-not (:done? (crux/entity (crux/db crux) ::watdiv-ingestion-status))
      (let [time-before (Date.)
            submit-future (future
                            (with-open [in (io/input-stream (io/resource resource))]
                              (rdf/submit-ntriples (:tx-log crux) in 1000)))]
        (assert (= 521585 @submit-future))
        (let [kafka-ingest-done (Date.)
              {:keys [crux.tx/tx-time]}
              (crux/submit-tx
                crux
                [[:crux.tx/put
                  {:crux.db/id ::watdiv-ingestion-status :done? false}]])]
          (crux/db crux tx-time tx-time) ;; block until indexed
          (crux/db
            crux (Date.)
            (:crux.tx/tx-time
             (crux/submit-tx
               crux
               [[:crux.tx/put
                 {:crux.db/id ::watdiv-ingestion-status
                  :watdiv/ingest-start-time time-before
                  :watdiv/kafka-ingest-time (- (.getTime kafka-ingest-done) (.getTime time-before))
                  :watdiv/ingest-time (- (.getTime (Date.)) (.getTime time-before))
                  :done? true}]]))))))))

(defmethod start-watdiv-runner :crux
  [_ {:keys [crux]}]
  (map->CruxBackend {:crux crux}))

(defrecord WatdivRunner [running-future]
  Closeable
  (close [_]
    (future-cancel running-future)))

;; TODO name the resulting file based on what test was run!
;;      and how many tests that were run!
(defn upload-watdiv-results
  [^File out-file]
  (s3/put-object
    :bucket-name (System/getenv "CRUX_BENCHMARK_BUCKET")
    :key (.getName out-file)
    :acl :public-read
    :file out-file)
  (s3/set-object-acl
    (System/getenv "CRUX_BENCHMARK_BUCKET")
    (.getName out-file)
    CannedAccessControlList/PublicRead))

(def watdiv-tests
  {"watdiv/data/watdiv-stress-100/warmup.1.desc" "watdiv/data/watdiv-stress-100/warmup.sparql"
   "watdiv/data/watdiv-stress-100/test.1.desc" "watdiv/data/watdiv-stress-100/test.1.sparql"
   "watdiv/data/watdiv-stress-100/test.2.desc" "watdiv/data/watdiv-stress-100/test.2.sparql"
   "watdiv/data/watdiv-stress-100/test.3.desc" "watdiv/data/watdiv-stress-100/test.3.sparql"
   "watdiv/data/watdiv-stress-100/test.4.desc" "watdiv/data/watdiv-stress-100/test.4.sparql"})

(defn execute-stress-test
  [backend tests-run out-file num-tests ^Long num-threads]
  (let [all-jobs-submitted (atom false)
        all-jobs-completed (atom false)
        pool (java.util.concurrent.Executors/newFixedThreadPool (inc num-threads))
        job-queue (java.util.concurrent.LinkedBlockingQueue. num-threads)
        completed-queue (java.util.concurrent.LinkedBlockingQueue. ^Long (* 10 num-threads))
        writer-future (.submit
                        pool
                        ^Runnable
                        (fn write-result-thread []
                          (with-open [out (io/writer out-file)]
                            (.write out "{\n")
                            (.write out (str ":test-time " (pr-str (System/currentTimeMillis)) "\n"))
                            (.write out (str ":backend-info " (pr-str (backend-info backend)) "\n"))
                            (.write out (str ":num-tests " (pr-str num-tests) "\n"))
                            (.write out (str ":num-threads " (pr-str num-threads) "\n"))
                            (.write out (str ":tests " "\n"))
                            (.write out "[\n")
                            (loop []
                              (when-let [{:keys [idx q error results time]} (if @all-jobs-completed
                                                                              (.poll completed-queue)
                                                                              (.take completed-queue))]
                                (.write out "{")
                                (.write out (str ":idx " (pr-str idx) "\n"))
                                (.write out (str ":query " (pr-str q) "\n"))
                                (if error
                                  (.write out (str ":error " (pr-str (str error)) "\n"))
                                  (.write out (str ":backend-results " results "\n")))
                                (.write out (str ":time " (pr-str time)))
                                (.write out "}\n")
                                (.flush out)
                                (recur)))
                            (.write out "]}"))))

        job-features (vec (for [i (range num-threads)]
                            (.submit
                              pool
                              ^Runnable
                              (fn run-jobs []
                                (when-let [{:keys [idx q] :as job} (if @all-jobs-submitted
                                                                     (.poll job-queue)
                                                                     (.take job-queue))]
                                  (let [start-time (System/currentTimeMillis)
                                        result
                                        (try
                                          {:results (execute-with-timeout backend (sparql/sparql->datalog q))}
                                          (catch java.util.concurrent.TimeoutException t
                                            {:error t})
                                          (catch IllegalStateException t
                                            {:error t})
                                          (catch Throwable t
                                            (log/error t "unkown error running watdiv tests")
                                            ;; datomic wrapps the error multiple times
                                            ;; doing this to get the cause exception!
                                            (when-not (instance? java.util.concurrent.TimeoutException
                                                                 (.getCause (.getCause (.getCause t))))
                                              (throw t))))]
                                    (.put completed-queue
                                          (merge
                                            job result
                                            {:time (- (System/currentTimeMillis) start-time)}))
                                    (recur)))))))]
    (try
      (with-open [desc-in (io/reader (io/resource "watdiv/data/watdiv-stress-100/test.1.desc"))
                  sparql-in (io/reader (io/resource "watdiv/data/watdiv-stress-100/test.1.sparql"))]
        (doseq [[idx [d q]] (->> (map vector (line-seq desc-in) (line-seq sparql-in))
                                 (take (or num-tests 100))
                                 (map-indexed vector))]
          (.put job-queue {:idx idx :q q})))
      (reset! all-jobs-submitted true)
      (doseq [^java.util.concurrent.Future f job-features] (.get f))
      (reset! all-jobs-completed true)
      (.get writer-future)

      (catch InterruptedException e
        (.shutdownNow pool)
        (throw e)))))

(defn run-watdiv-test
  [backend num-tests num-threads]
  (let [status (atom nil)
        tests-run (atom 0)
        out-file (io/file (format "watdiv_%s.edn" (System/currentTimeMillis)))]
    (map->WatdivRunner
      {:status status
       :tests-run tests-run
       :out-file out-file
       :num-tests num-tests
       :num-threads num-threads
       :backend backend
       :running-future
       (future
         (try
           (reset! status :ingesting-watdiv-data)
           (ingest-watdiv-data backend "watdiv/data/watdiv.10M.nt")
           (reset! status :running-benchmark)
           (execute-stress-test backend tests-run out-file num-tests num-threads)
           (reset! status :uploading-results)
           (upload-watdiv-results out-file)
           (reset! status :benchmark-completed)
           (catch Throwable t
             (log/error t "watdiv testrun failed")
             (reset! status :benchmark-failed)
             false)))})))

(defn start-and-run
  [backend-name node num-tests num-threads]
  (let [backend (start-watdiv-runner backend-name node)]
    (run-watdiv-test backend num-tests num-threads)))
