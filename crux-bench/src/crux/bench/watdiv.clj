(ns crux.bench.watdiv
  (:require [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [crux.bench :as bench]
            [crux.api :as crux]
            [crux.index :as idx]
            [crux.rdf :as rdf]
            [crux.sparql :as sparql]
            [datomic.api :as d]
            [crux.io :as cio]
            [clojure.string :as string])
  (:import [java.io Closeable File StringReader]
           java.util.Date
           org.eclipse.rdf4j.query.Binding
           org.eclipse.rdf4j.repository.RepositoryConnection
           org.eclipse.rdf4j.repository.sail.SailRepository
           org.eclipse.rdf4j.rio.RDFFormat
           org.eclipse.rdf4j.sail.nativerdf.NativeStore))

(defn output-to-file [out output]
  (spit out (str output "\n") :append true))

(def watdiv-tests
  {"watdiv-stress-100/warmup.1.desc" "watdiv-stress-100/warmup.sparql"
   "watdiv-stress-100/test.1.desc" "watdiv-stress-100/test.1.sparql"
   "watdiv-stress-100/test.2.desc" "watdiv-stress-100/test.2.sparql"
   "watdiv-stress-100/test.3.desc" "watdiv-stress-100/test.3.sparql"
   "watdiv-stress-100/test.4.desc" "watdiv-stress-100/test.4.sparql"})

(def query-timeout-ms 30000)

(def watdiv-input-file (io/resource "watdiv.10M.nt"))

(defn execute-stress-test
  [conn query-fn test-name num-tests ^Long num-threads]
  (bench/run-bench (keyword test-name)
                   (let [all-jobs-submitted (atom false)
                         all-jobs-completed (atom false)
                         pool (java.util.concurrent.Executors/newFixedThreadPool (inc num-threads))
                         job-queue (java.util.concurrent.LinkedBlockingQueue. num-threads)
                         job-features (mapv (fn [_]
                                              (.submit
                                               pool
                                               ^Runnable
                                               (fn run-jobs []
                                                 (when-let [{:keys [idx q]} (if @all-jobs-submitted
                                                                              (.poll job-queue)
                                                                              (.take job-queue))]
                                                   (let [start-time (System/currentTimeMillis)
                                                         result
                                                         (try
                                                           {:result-count (count (query-fn conn q))}
                                                           (catch java.util.concurrent.TimeoutException t
                                                             {:error (.getMessage t)}))
                                                         output (merge {:query-index idx
                                                                        :time-taken-ms (- (System/currentTimeMillis) start-time)}
                                                                       result)]
                                                     (output-to-file (str test-name ".edn") output))
                                                   (recur)))))
                                            (range num-threads))]
                     (try
                       (with-open [desc-in (io/reader (io/resource "watdiv-stress-100/test.1.desc"))
                                   sparql-in (io/reader (io/resource "watdiv-stress-100/test.1.sparql"))]
                         (doseq [[idx [_ q]] (->> (map vector (line-seq desc-in) (line-seq sparql-in))
                                                  (take num-tests)
                                                  (map-indexed vector))]
                           (.put job-queue {:idx idx :q q})))
                       (reset! all-jobs-submitted true)
                       (doseq [^java.util.concurrent.Future f job-features] (.get f))
                       (reset! all-jobs-completed true)
                       (.shutdownNow pool)
                       (catch InterruptedException e
                         (.shutdownNow pool)
                         (throw e))))))

;; Datomic bench ===

(def datomic-tx-size 100)

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

(defn with-datomic [f]
  (let [uri (str "datomic:free://"
                 (or (System/getenv "DATOMIC_TRANSACTOR_URI") "datomic")
                 ":4334/bench?password=password")]
    (try
        (d/delete-database uri)
        (d/create-database uri)
        (with-open [conn (d/connect uri)]
          @(d/transact conn datomic-watdiv-schema)
          (f conn))
        (finally
          (d/delete-database uri)))))

(defn load-rdf-into-datomic [conn]
  (bench/run-bench :ingest-datomic
    (with-open [in (io/input-stream (watdiv-input-file))]
      (->> (rdf/ntriples-seq in)
           (rdf/statements->maps)
           (map #(rdf/use-default-language % rdf/*default-language*))
           (partition-all datomic-tx-size)
           (reduce (fn [^long n entities]
                     (let [done? (atom false)]
                       (while (not @done?)
                         (try
                           @(d/transact conn (mapcat entity->idents entities))
                           @(d/transact conn (->> (map entity->datomic entities)
                                                  (apply concat)
                                                  (vec)))
                           (reset! done? true)))
                       (+ n (count entities))))
                   0)))))

(defn run-watdiv-bench-datomic [{:keys [test-count thread-count]}]
  (with-datomic
    (fn [conn]
      (bench/with-bench-ns :watdiv
        (load-rdf-into-datomic conn)
        (execute-stress-test
         conn
         (fn [conn q] (d/query {:query (sparql/sparql->datalog q)
                                :timeout query-timeout-ms
                                :args [(d/db conn)]}))
         "stress-test-datomic"
         test-count
         thread-count)))))

;; rdf bench ===

(defn with-sail-repository [f]
  (let [db-dir (str (cio/create-tmpdir "sail-store"))
        db (SailRepository. (NativeStore. (io/file db-dir)))]
    (try
      (.initialize db)
      (with-open [conn (.getConnection db)]
        (f conn))
      (finally
        (.shutDown db)
        (cio/delete-dir db-dir)))))

(defn load-rdf-into-sail [^RepositoryConnection conn]
  (bench/run-bench :ingest-rdf
    (with-open [in (io/input-stream watdiv-input-file)]
      {:entity-count
       (->> (partition-all rdf/*ntriples-log-size* (line-seq (io/reader in)))
            (reduce (fn [n chunk]
                      (.add conn (StringReader. (string/join "\n" chunk)) "" RDFFormat/NTRIPLES rdf/empty-resource-array)
                      (+ n (count chunk)))
                    0))})))

(defn execute-sparql [^RepositoryConnection conn q]
  (with-open [tq (.evaluate (doto (.prepareTupleQuery conn q)
                              (.setMaxExecutionTime (quot query-timeout-ms 1000))))]
    (set ((fn step []
            (when (.hasNext tq)
              (cons (mapv #(rdf/rdf->clj (.getValue ^Binding %))
                          (.next tq))
                    (lazy-seq (step)))))))))

(defn run-watdiv-bench-rdf [{:keys [test-count thread-count]}]
  (with-sail-repository
    (fn [conn]
      (bench/with-bench-ns :watdiv
        (load-rdf-into-sail conn)
        (execute-stress-test
         conn
         (fn [conn q] (execute-sparql conn q))
         "stress-test-rdf"
         test-count
         thread-count)))))

;; crux bench ===

(defn ingest-crux
  [node]
  (bench/run-bench :ingest
                   (let [{:keys [last-tx entity-count]}
                         (with-open [in (io/input-stream watdiv-input-file)]
                           (rdf/submit-ntriples node in 1000))]
                     (crux/await-tx node last-tx)
                     {:entity-count entity-count})))


(defn run-watdiv-bench-crux
  [node {:keys [test-count thread-count]}]
  (bench/with-bench-ns :watdiv
    (ingest-crux node)
    (execute-stress-test
     node
     (fn [node q] (crux/q
                   (crux/db node)
                   (sparql/sparql->datalog q)))
     "stress-test-crux"
     test-count
     thread-count)))
