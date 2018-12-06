(ns crux.watdiv-test
  (:require [clojure.test :as t]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [crux.db :as db]
            [crux.index :as idx]
            [crux.tx :as tx]
            [crux.lru :as lru]
            [crux.rdf :as rdf]
            [crux.query :as q]
            [crux.sparql :as sparql]
            [crux.kafka :as k]
            [crux.fixtures :as f])
  (:import [java.util Date]))

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

;; Second test run, with -Xmss10Mb, from lein:
;; "Elapsed time: 3255993.121931 msecs"
;; Ran 1 tests containing 12401 assertions.
;; 0 failures, 186 errors.

(def ^:const watdiv-triples-resource "watdiv/watdiv.10M.nt")
(def ^:const watdiv-num-queries nil)
(def ^:const watdiv-indexes nil)

(def run-watdiv-tests? (and false (boolean (io/resource watdiv-triples-resource))))

(defn with-watdiv-data [f]
  (if run-watdiv-tests?
    (with-open [in (io/input-stream (io/resource watdiv-triples-resource))]
      (let [tx-topic "test-can-run-watdiv-tx-queries"
            doc-topic "test-can-run-watdiv-doc-queries"
            tx-log (k/->KafkaTxLog f/*producer* tx-topic doc-topic {})
            object-store (lru/new-cached-object-store f/*kv*)
            indexer (tx/->KvIndexer f/*kv* tx-log object-store)]

        (k/create-topic f/*admin-client* tx-topic 1 1 k/tx-topic-config)
        (k/create-topic f/*admin-client* doc-topic 1 1 k/doc-topic-config)
        (k/subscribe-from-stored-offsets indexer f/*consumer* [tx-topic doc-topic])

        (time
         (let [submit-future (future (rdf/submit-ntriples tx-log in 1000))
               consume-args {:indexer indexer
                             :consumer f/*consumer*
                             :tx-topic tx-topic
                             :doc-topic doc-topic}]
           (k/consume-and-index-entities consume-args)
           (while (not= {:txs 0 :docs 0}
                        (k/consume-and-index-entities
                         (assoc consume-args :timeout 100))))
           (t/is (= 521585 @submit-future))))
        (f)))
    (f)))

(t/use-fixtures :once f/with-embedded-kafka-cluster f/with-kafka-client f/with-kv-store with-watdiv-data)

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
                         (contains? watdiv-indexes idx))
               :let [start-time (System/currentTimeMillis)]]
         (.write out "{")
         (.write out (str ":idx " (pr-str idx) "\n"))
         (t/is (try
                 (.write out (str ":query " (pr-str q) "\n"))
                 (.write out (str ":results " (pr-str (count (q/q (q/db f/*kv*)
                                                                  (sparql/sparql->datalog q))))
                                  "\n"))
                 true
                 (catch Throwable t
                   (.write out (str ":error " (pr-str (str t)) "\n"))
                   (throw t))))
         (.write out (str ":time " (pr-str (-  (System/currentTimeMillis) start-time))))
         (.write out "}\n")
         (.flush out))
       (.write out "]")))
    (t/is true "skipping")))
