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

(defmulti start-watdiv-runner
  (fn [key system] key))

(def query-timeout-ms 15000)

(defprotocol WatdivBackend
  (execute-with-timeout [this datalog])
  (injest-watdiv-data [this resource]))

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
                   (when (zero? (long (mod n rdf/*ntriples-log-size*)))
                     (log/debug "submitted" n))
                   @(d/transact conn (mapcat entity->idents entities))
                   @(d/transact conn (->> (map entity->datomic entities)
                                          (apply concat)
                                          (vec)))
                   (+ n (count entities)))
                 0))))

(defrecord DatomicBackend [conn]
  WatdivBackend
  (execute-with-timeout [this datalog]
    (d/query {:query datalog
              :timeout query-timeout-ms
              :args [(d/db conn)]}))
  (injest-watdiv-data [this resource]
    (when-not (d/entity (d/db conn) [:watdiv/injest-state :global])
      (log/info "starting to injest watdiv data into datomic")
      (let [time-before (Date.)]
        (load-rdf-into-datomic conn resource)
        (let [injest-time (- (.getTime (Date.)) (.getTime time-before))]
          (log/infof "completed datomic watdiv injestion time taken: %s" injest-time)
          @(d/transact [{:watdiv/injest-state :global
                         :watdiv/injest-time injest-time}]))))))

;; See: https://dsg.uwaterloo.ca/watdiv/watdiv-data-model.txt
;; Some things like dates are strings in the actual data.
(def datomic-watdiv-schema
  [#:db{:valueType :db.type/keyword
        :cardinality :db.cardinality/one
        :indexed true
        :ident :watdiv/injest-state}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident :watdiv/injest-time}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/composer")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/follows")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/friendOf")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/one
        :ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/gender")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/hasGenre")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/hits")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/likes")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/makesPurchase")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/purchaseDate")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/one
        :ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/purchaseFor")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/subscribes")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://db.uwaterloo.ca/~galuc/wsdbm/userId")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://ogp.me/ns#tag")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://ogp.me/ns#title")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/dc/terms/Location")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/goodrelations/description")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/goodrelations/includes")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/goodrelations/name")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://purl.org/goodrelations/offers")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/goodrelations/price")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/goodrelations/serialNumber")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/goodrelations/validFrom")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/goodrelations/validThrough")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/ontology/mo/artist")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/ontology/mo/conductor")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/ontology/mo/movement")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/ontology/mo/opus")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/ontology/mo/performed_in")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/ontology/mo/performer")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/ontology/mo/producer")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/ontology/mo/record_number")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/ontology/mo/release")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://purl.org/stuff/rev#hasReview")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/stuff/rev#rating")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/stuff/rev#reviewer")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/stuff/rev#text")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/stuff/rev#title")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://purl.org/stuff/rev#totalVotes")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://schema.org/actor")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/aggregateRating")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://schema.org/author")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/many
        :ident (keyword "http://schema.org/award")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/birthDate")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/bookEdition")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/caption")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/contactPoint")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/contentRating")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/contentSize")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/datePublished")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/description")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/director")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/duration")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://schema.org/editor")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/eligibleQuantity")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://schema.org/eligibleRegion")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/email")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://schema.org/employee")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/expires")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/faxNumber")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/isbn")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/jobTitle")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/keywords")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://schema.org/language")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/legalName")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/nationality")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/numberOfPages")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/openingHours")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/paymentAccepted")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/priceValidUntil")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/printColumn")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/printEdition")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/printPage")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/printSection")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/producer")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/publisher")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/telephone")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/text")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://schema.org/trailer")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/url")}
   #:db{:valueType :db.type/long
        :cardinality :db.cardinality/one
        :ident (keyword "http://schema.org/wordCount")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/one
        :ident (keyword "http://www.geonames.org/ontology#parentCountry")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/many
        :ident (keyword "http://www.w3.org/1999/02/22-rdf-syntax-ns#type")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/one
        :ident (keyword "http://xmlns.com/foaf/age")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://xmlns.com/foaf/familyName")}
   #:db{:valueType :db.type/string
        :cardinality :db.cardinality/one
        :ident (keyword "http://xmlns.com/foaf/givenName")}
   #:db{:valueType :db.type/ref
        :cardinality :db.cardinality/one
        :ident (keyword "http://xmlns.com/foaf/homepage")}])

(defmethod start-watdiv-runner :datomic
  [_ system]
  (let [conn (d/connect "datomic:free://datomic:4334/bench")]
    @(d/transact conn datomic-watdiv-schema)
    (map->DatomicBackend {:conn conn})))

(defrecord CruxBackend [crux]
  WatdivBackend
  (execute-with-timeout [this datalog]
    (let [db (crux/db crux)]
      (with-open [snapshot (crux/new-snapshot db)]
        (let [query-future (future (count (crux/q db snapshot datalog)))]
          (or (deref query-future query-timeout-ms nil)
              (do (future-cancel query-future)
                  (throw (IllegalStateException. "Query timed out."))))))))

  (injest-watdiv-data [this resource]
    (let [submit-future (future
                          (with-open [in (io/input-stream (io/resource resource))]
                            (rdf/submit-ntriples (:tx-log crux) in 1000)))]
      (assert (= 521585 @submit-future))
      (crux/submit-tx crux [[:crux.tx/put
                             ::watdiv-injestion-status
                             {:crux.db/id ::watdiv-injestion-status
                              :done? true}]])
      (crux/sync crux (Duration/ofSeconds 6000)))))

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
  [backend tests-run out-file num-tests]
  (with-open [desc-in (io/reader (io/resource "watdiv/data/watdiv-stress-100/test.1.desc"))
              sparql-in (io/reader (io/resource "watdiv/data/watdiv-stress-100/test.1.sparql"))
              out (io/writer out-file)]
    (.write out "[\n")
    (doseq [[idx [d q]] (->> (map vector (line-seq desc-in) (line-seq sparql-in))
                             (take (or num-tests 100))
                             (map-indexed vector))]
      (.write out "{")
      (.write out (str ":idx " (pr-str idx) "\n"))
      (.write out (str ":query " (pr-str q) "\n"))
      (let [start-time (System/currentTimeMillis)]
        (try
          (.write out (str ":backend-results " (execute-with-timeout backend (sparql/sparql->datalog q))
                           "\n"))
          (catch IllegalStateException t
            (.write out (str ":crux-error " (pr-str (str t)) "\n")))
          (catch Throwable t
            (.write out (str ":crux-error " (pr-str (str t)) "\n"))
            (throw t)))
        (.write out (str ":crux-time " (pr-str (-  (System/currentTimeMillis) start-time)))))

      (.write out "}\n")
      (.flush out)
      (swap! tests-run inc))
    (.write out "]")))

(defn run-watdiv-test
  [backend num-tests]
  (let [status (atom nil)
        tests-run (atom 0)
        out-file (io/file (format "watdiv_%s.edn" (System/currentTimeMillis)))]
    (map->WatdivRunner
      {:status status
       :tests-run tests-run
       :out-file out-file
       :num-tests num-tests
       :backend backend
       :running-future
       (future
         (try
           (reset! status :injesting-watdiv-data)
           (injest-watdiv-data backend "watdiv/data/watdiv.10M.nt")
           (reset! status :running-benchmark)
           (execute-stress-test backend tests-run out-file num-tests)
           (reset! status :uploading-results)
           (upload-watdiv-results out-file)
           (reset! status :benchmark-completed)
           (catch Throwable t
             (log/error t "watdiv testrun failed")
             (reset! status :benchmark-failed)
             false)))})))

(defn start-and-run
  [backend-name system num-tests]
  (let [backend (start-watdiv-runner backend-name system)]
    (run-watdiv-test backend num-tests)))
