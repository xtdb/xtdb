(ns xtdb.bench.ingest-tx-overhead
  (:require [clojure.string :as s]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.lang AutoCloseable]
           [java.sql Connection]
           [xtdb.api Xtdb Xtdb$Connection]))

(defprotocol DoIngest
  "Ingest `doc-count` documents into `table`, in transactions of `per-batch`, by whichever route
   the receiver has. Returns once every document is queryable."
  (do-ingest [this table doc-count per-batch]))

(extend-protocol DoIngest
  Xtdb
  (do-ingest [node table doc-count per-batch]
    (with-open [conn (.connect node)]
      (do-ingest conn table doc-count per-batch)))

  Connection
  (do-ingest [conn table doc-count per-batch]
    (with-open [ps (jdbc/prepare conn [(format "INSERT INTO %s (_id) VALUES (?)" (name table))])]
      (doseq [batch (partition-all per-batch (range doc-count))]
        (when (zero? (mod (first batch) 1000))
          (log/trace :done (first batch)))

        (when (Thread/interrupted) (throw (InterruptedException.)))

        (jdbc/with-transaction [_ conn]
          (jdbc/execute-batch! ps (mapv vector batch)))))

    (let [{actual :doc_count} (jdbc/execute-one! conn [(format "SELECT COUNT(*) doc_count FROM %s" (name table))])]
      (assert (= actual doc-count)
              (format "failed for %s: expected: %d, got: %d" (name table) doc-count actual))))

  Xtdb$Connection
  (do-ingest [conn table doc-count per-batch]
    (doseq [batch (partition-all per-batch (range doc-count))]
      (when (Thread/interrupted) (throw (InterruptedException.)))

      (when (zero? (mod (first batch) 1000))
        (log/trace :done (first batch)))

      (xt/submit-tx conn [(into [:put-docs table] (map (fn [idx] {:xt/id idx})) batch)]))

    ;; nothing above awaits, so without this the stage would time only the submits
    (xt/execute-tx conn [])

    (let [[{actual :doc-count}] (xt/q conn (format "SELECT COUNT(*) doc_count FROM %s" (name table)))]
      (assert (= actual doc-count)
              (format "failed for %s: expected: %d, got: %d" (name table) doc-count actual)))))

(defmethod b/cli-flags :ingest-tx-overhead [_]
  [["-dc" "--doc-count DOCUMENT_COUNT" "Number of documents to ingest"
    :parse-fn parse-long
    :default 100000]

   ["-bs" "--batch-sizes BATCH_SIZES" "Batch sizes to use for ingestion, e.g. \"1000,100,10,1\" (currently supported: 1000, 100, 10, 1)"
    :parse-fn #(->> (s/split % #",") (map parse-long) (into #{}))
    :default #{1000 100 10 1}]

   ["-h" "--help"]])

(defn benchmark [{:keys [seed doc-count batch-sizes],
                  :or {seed 0, doc-count 100000, batch-sizes #{1000 100 10 1}}}]
  (log/info {:doc-count doc-count :batch-sizes batch-sizes})

  {:title "Ingest batch vs individual"
   :seed seed
   :parameters {:doc-count doc-count :batch-sizes (sort > batch-sizes)}
   :tasks (for [batch-size (sort > batch-sizes)]
            {:t :call
             :batch-size batch-size
             :stage (keyword (str "ingest-batch-" batch-size))
             :f (fn [{:keys [node]}]
                  (do-ingest node (keyword (str "batched_" batch-size)) doc-count batch-size))})})

(defmethod b/->benchmark :ingest-tx-overhead [_ {:keys [doc-count batch-sizes] :as opts}]
  (log/info {:doc-count doc-count :batch-sizes batch-sizes})
  (benchmark opts))

(comment
  ;; xt-pgwire - going through pg-wire, one connection
  ;; xt-adbc - using an in-process ADBC connection
  ;; xt-local - same as xt-adbc but backed by disk
  ;; pg-conn - talking to real postgres
  (let [system-type :xt-pgwire
        f (b/compile-benchmark (benchmark {:batch-sizes #{1000 100 10 1}, :doc-count 1000000}))]
    (with-open [^AutoCloseable
                node (case system-type
                       (:xt-pgwire :xt-adbc) (xtn/start-node)

                       :xt-local (let [path (util/->path "/tmp/xt-tx-overhead-bench")]
                                   (util/delete-dir path)
                                   (tu/->local-node {:node-dir path}))

                       :pg-conn (jdbc/get-connection {:dbtype "postgresql"
                                                      :dbname "postgres"
                                                      :user "postgres"
                                                      :password "postgres"}))]

      (case system-type
        (:xt-adbc :xt-local)
        (f node)

        (:xt-pgwire :pg-conn)
        (with-open [conn (jdbc/get-connection node)]
          (f conn))))

    #_
    (f dev/node)))

