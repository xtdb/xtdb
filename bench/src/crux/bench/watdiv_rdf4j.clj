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
      {:success? true
       :entity-count
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

(defn run-watdiv-bench [{:keys [test-count] :as opts}]
  (with-sail-repository
    (fn [conn]
      (bench/with-bench-ns :watdiv-rdf
        (load-rdf-into-sail conn)
        (watdiv/with-watdiv-queries watdiv/watdiv-stress-100-1-sparql
          (fn [queries]
            (-> queries
                (cond->> test-count (take test-count))
                (->> (bench/with-thread-pool opts
                       (fn [{:keys [idx q]}]
                         (bench/with-dimensions {:query-idx idx}
                           (bench/run-bench (format "query-%d" idx)
                                            {:result-count (count (execute-sparql conn q))}))))))))))))

(comment
  (with-redefs [watdiv/watdiv-input-file (io/resource "watdiv.10.nt")]
    (bench/save-to-file (io/file "rdf4j-results.edn")
                        (->> (run-watdiv-bench {:test-count 10})
                             (filter :query-idx)
                             (sort-by :query-idx)))))

(defn -main []
  (let [output-file (io/file "rdf4j-results.edn")
        watdiv-results (run-watdiv-bench {:test-count 100})]
    (bench/save-to-file output-file (cons (first watdiv-results)
                                          (->> (rest watdiv-results)
                                               (filter :query-idx)
                                               (sort-by :query-idx))))
    (bench/save-to-s3 {:database "rdf4j" :version "3.0.0"} output-file)))
