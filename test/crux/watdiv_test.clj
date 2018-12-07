(ns crux.watdiv-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.tools.logging :as log]
            [clojure.walk :as w]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.io :as cio]
            [crux.tx :as tx]
            [crux.lru :as lru]
            [crux.rdf :as rdf]
            [crux.query :as q]
            [crux.sparql :as sparql]
            [crux.kafka :as k]
            [crux.fixtures :as f]
            [datascript.core :as d])
  (:import java.util.Date
           java.io.InputStream
           org.eclipse.rdf4j.repository.sail.SailRepository
           org.eclipse.rdf4j.repository.RepositoryConnection
           org.eclipse.rdf4j.sail.nativerdf.NativeStore
           org.eclipse.rdf4j.rio.RDFFormat
           org.eclipse.rdf4j.query.Binding))

;; See:
;; https://dsg.uwaterloo.ca/watdiv/

;; Needs the following files downloaded and unpacked under test/watdiv
;; in the project root:

;; https://dsg.uwaterloo.ca/watdiv/watdiv.10M.tar.bz2
;; https://dsg.uwaterloo.ca/watdiv/stress-workloads.tar.gz

;; First test run:

;; WatDiv 10M:
;; wc -l test/watdiv/watdiv.10M.nt
;; 10916457 test/watdiv/watdiv.10M.nt
;; du -hs test/watdiv/watdiv.10M.nt
;; 1.5G	test/watdiv/watdiv.10M.nt

;; Ingest:
;; "Elapsed time: 136125.904116 msecs"
;; du -hs /tmp/kafka-log* /tmp/kv-store*
;; 672M	/tmp/kafka-log1198983040100044874
;; 192M	/tmp/kv-store1625659699196661317

;; Query:
;; wc -l test/watdiv/watdiv-stress-100/test.1.sparql
;; 12400 test/watdiv/watdiv-stress-100/test.1.sparql

;; "Elapsed time: 2472368.881591 msecs"
;; Tested 1 namespaces
;; Ran 12401 assertions, in 1 test functions
;; 518 errors

;; Second test run, with -Xmss10M, from lein:
;; "Elapsed time: 3255993.121931 msecs"
;; Ran 1 tests containing 12401 assertions.
;; 0 failures, 186 errors.

;; Thrid test run, with -Xmss32M, from lein trampoline:
;; lein test crux.watdiv-test
;; "Elapsed time: 130545.461865 msecs"
;; "Elapsed time: 3729324.4121 msecs"

;; Ran 1 tests containing 12401 assertions.
;; 0 failures, 0 errors.

(def ^:const watdiv-triples-resource "watdiv/watdiv.10M.nt")
(def ^:const watdiv-num-queries nil)
(def ^:const watdiv-indexes nil)

(def run-watdiv-tests? (and false (boolean (io/resource watdiv-triples-resource))))

(def crux-tests? true)
(def datascript-tests? false)
(def sail-tests? false)

(def ^:dynamic *conn*)
(def ^:dynamic *kw->id*)
(def ^:dynamic *id->kw*)

(def ^:dynamic *sail-conn*)

(defn entity->datascript [kw->id e]
  (let [id-fn (fn [kw]
                (get (swap! kw->id update kw (fn [x]
                                               (or x (inc (count @kw->id)))))
                     kw))
        id (id-fn (:crux.db/id e))
        tx-op-fn (fn tx-op-fn [k v]
                   (cond
                     (keyword? v)
                     [[:db/add id k (id-fn v)]]

                     (set? v)
                     (vec (mapcat #(tx-op-fn k %) v))

                     :else
                     [[:db/add id k v]]))]
    (->> (for [[k v] (dissoc e :crux.db/id)]
           (tx-op-fn k v))
         (apply concat)
         (vec))))

(defn submit-ntriples-to-datascript [conn kw->id in tx-size]
  (->> (rdf/ntriples-seq in)
       (rdf/statements->maps)
       (map #(rdf/use-default-language % rdf/*default-language*))
       (partition-all tx-size)
       (reduce (fn [^long n entities]
                 (when (zero? (long (mod n rdf/*ntriples-log-size*)))
                   (log/debug "submitted" n))
                 (let [tx-ops (->> (for [entity entities]
                                     (entity->datascript kw->id entity))
                                   (apply concat)
                                   (vec))]
                   (d/transact! conn tx-ops))
                 (+ n (count entities)))
               0)))

(defn execute-sail-sparql [^RepositoryConnection conn q]
  (with-open [tq (.evaluate (.prepareTupleQuery conn q))]
    (set ((fn step []
            (when (.hasNext tq)
              (cons (mapv #(rdf/rdf->clj (.getValue ^Binding %))
                          (.next tq))
                    (lazy-seq (step)))))))))

(defn load-rdf-into-sail [conn ^InputStream in]
  (.add ^RepositoryConnection conn in "" RDFFormat/NTRIPLES rdf/empty-resource-array))

(defn with-sail-repository [f]
  (let [db-dir (str (cio/create-tmpdir "sail-store"))
        db (SailRepository. (doto (NativeStore. (io/file db-dir))
                              (.setForceSync true)))]
    (try
      (.initialize db)
      (with-open [conn (.getConnection db)]
        (binding [*sail-conn* conn]
          (f)))
      (finally
        (.shutDown db)
        (cio/delete-dir db-dir)))))

(defn with-watdiv-data [f]
  (if run-watdiv-tests?
    (let [tx-topic "test-can-run-watdiv-tx-queries"
          doc-topic "test-can-run-watdiv-doc-queries"
          tx-log (k/->KafkaTxLog f/*producer* tx-topic doc-topic {})
          object-store (lru/new-cached-object-store f/*kv*)
          indexer (tx/->KvIndexer f/*kv* tx-log object-store)
          conn (d/create-conn)
          kw->id (atom {})]

      (k/create-topic f/*admin-client* tx-topic 1 1 k/tx-topic-config)
      (k/create-topic f/*admin-client* doc-topic 1 1 k/doc-topic-config)
      (k/subscribe-from-stored-offsets indexer f/*consumer* [tx-topic doc-topic])

      (when datascript-tests?
        (println "Loading into Datascript...")
        (time
         (with-open [in (io/input-stream (io/resource watdiv-triples-resource))]
           (submit-ntriples-to-datascript conn kw->id in 1000))))

      (when sail-tests?
        (println "Loading into Sail...")
        (time
         (with-open [in (io/input-stream (io/resource watdiv-triples-resource))]
           (load-rdf-into-sail *sail-conn* in))))

      (when crux-tests?
        (println "Loading into Crux...")
        (time
         (let [submit-future (future
                               (with-open [in (io/input-stream (io/resource watdiv-triples-resource))]
                                 (rdf/submit-ntriples tx-log in 1000)))
               consume-args {:indexer indexer
                             :consumer f/*consumer*
                             :tx-topic tx-topic
                             :doc-topic doc-topic}]
           (k/consume-and-index-entities consume-args)
           (while (not= {:txs 0 :docs 0}
                        (k/consume-and-index-entities
                         (assoc consume-args :timeout 100))))
           (t/is (= 521585 @submit-future)))))

      (binding [*conn* conn
                *kw->id* @kw->id
                *id->kw* (set/map-invert @kw->id)]
        (f)))
    (f)))

(t/use-fixtures :once f/with-embedded-kafka-cluster f/with-kafka-client with-sail-repository f/with-kv-store with-watdiv-data)

;; TODO: What do the numbers in the .desc file represent? They all
;; add up to the same across test runs, so cannot be query
;; times. Does not seem to be result size either.
(t/deftest watdiv-stress-test-1
  (if run-watdiv-tests?
    (time
     (with-open [desc-in (io/reader (io/resource "watdiv/watdiv-stress-100/test.1.desc"))
                 sparql-in (io/reader (io/resource "watdiv/watdiv-stress-100/test.1.sparql"))
                 out (io/writer (io/file (format "target/watdiv_%s.edn" (System/currentTimeMillis))))]
       (.write out "[\n")
       (doseq [[idx [d q]] (->> (cond->> (map vector (line-seq desc-in) (line-seq sparql-in))
                                  watdiv-num-queries (take watdiv-num-queries))
                                (map-indexed vector))
               :when (or (nil? watdiv-indexes)
                         (contains? watdiv-indexes idx))]
         (.write out "{")
         (.write out (str ":idx " (pr-str idx) "\n"))
         (.write out (str ":query " (pr-str q) "\n"))
         (when crux-tests?
           (let [start-time (System/currentTimeMillis)]
             (t/is (try
                     (.write out (str ":crux-results " (pr-str (count (q/q (q/db f/*kv*)
                                                                           (sparql/sparql->datalog q))))
                                      "\n"))
                     true
                     (catch Throwable t
                       (.write out (str ":crux-error " (pr-str (str t)) "\n"))
                       (throw t))))
             (.write out (str ":crux-time " (pr-str (-  (System/currentTimeMillis) start-time))))))

         (when sail-tests?
           (let [start-time (System/currentTimeMillis)]
             (t/is (try
                     (.write out (str ":sail-results " (pr-str (count (execute-sail-sparql *conn* q)))
                                      "\n"))
                     true
                     (catch Throwable t
                       (.write out (str ":sail-error " (pr-str (str t)) "\n"))
                       (throw t))))
             (.write out (str ":sail-time " (pr-str (-  (System/currentTimeMillis) start-time))))))

         (when datascript-tests?
           (let [start-time (System/currentTimeMillis)]
             (t/is (try
                     (.write out (str ":datascript-results " (pr-str (count (w/postwalk-replace
                                                                             *id->kw*
                                                                             (d/q (w/postwalk-replace
                                                                                   *kw->id*
                                                                                   (sparql/sparql->datalog q))
                                                                                  @*conn*))))
                                      "\n"))
                     true
                     (catch Throwable t
                       (.write out (str ":datascript-error " (pr-str (str t)) "\n"))
                       (throw t))))
             (.write out (str ":datascript-time " (pr-str (-  (System/currentTimeMillis) start-time))))))
         (.write out "}\n")
         (.flush out))
       (.write out "]")))
    (t/is true "skipping")))

(t/deftest sail-sanity-check
  (with-sail-repository
    (fn []
      (with-open [in (io/input-stream (io/resource "crux/example-data-artists.nt"))]
        (load-rdf-into-sail *sail-conn* in))
      (t/is (= 2 (count (execute-sail-sparql *sail-conn* "
PREFIX ex: <http://example.org/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?s ?n
WHERE
{
   ?s a ex:Artist;
     foaf:firstName ?n.
}")))))))
