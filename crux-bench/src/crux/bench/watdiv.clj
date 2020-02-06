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
                                                           (catch Throwable t
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
                       {:num-tests num-tests :num-threads num-threads}
                       (catch InterruptedException e
                         (.shutdownNow pool)
                         (throw e))))))

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
