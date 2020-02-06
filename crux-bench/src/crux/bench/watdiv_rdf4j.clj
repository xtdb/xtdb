(ns crux.bench.watdiv-rdf4j
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [crux.bench :as bench]
            [crux.bench.watdiv :as watdiv]
            [crux.io :as cio]
            [crux.rdf :as rdf])
  (:import java.io.StringReader
           org.eclipse.rdf4j.query.Binding
           org.eclipse.rdf4j.repository.RepositoryConnection
           org.eclipse.rdf4j.repository.sail.SailRepository
           org.eclipse.rdf4j.rio.RDFFormat
           org.eclipse.rdf4j.sail.nativerdf.NativeStore))

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
    (with-open [in (io/input-stream watdiv/watdiv-input-file)]
      {:entity-count
       (->> (partition-all rdf/*ntriples-log-size* (line-seq (io/reader in)))
            (reduce (fn [n chunk]
                      (.add conn (StringReader. (string/join "\n" chunk)) "" RDFFormat/NTRIPLES rdf/empty-resource-array)
                      (+ n (count chunk)))
                    0))})))

(defn execute-sparql [^RepositoryConnection conn q]
  (with-open [tq (.evaluate (doto (.prepareTupleQuery conn q)
                              (.setMaxExecutionTime (quot watdiv/query-timeout-ms 1000))))]
    (set ((fn step []
            (when (.hasNext tq)
              (cons (mapv #(rdf/rdf->clj (.getValue ^Binding %))
                          (.next tq))
                    (lazy-seq (step)))))))))

(defn run-watdiv-bench [{:keys [test-count thread-count]}]
  (with-sail-repository
    (fn [conn]
      (bench/with-bench-ns :watdiv
        (load-rdf-into-sail conn)
        (watdiv/execute-stress-test
         conn
         (fn [conn q] (execute-sparql conn q))
         "stress-test-rdf"
         test-count
         thread-count)))))

(defn -main []
  (run-watdiv-bench {:thread-count 1 :test-count 100}))
