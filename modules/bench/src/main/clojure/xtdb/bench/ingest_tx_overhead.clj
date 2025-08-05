(ns xtdb.bench.ingest-tx-overhead
  (:require [clojure.tools.logging :as log]
            [clojure.string :as s]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.node :as xtn]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.lang AutoCloseable]
           [java.sql Connection]
           [xtdb.api Xtdb]))

(defprotocol DoIngest
  (do-ingest [this table doc-count per-batch]))

(extend-protocol DoIngest
  Connection
  (do-ingest [conn table ^long doc-count ^long per-batch]
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

  Xtdb
  (do-ingest [node table ^long doc-count ^long per-batch]
    (doseq [batch (partition-all per-batch (range doc-count))]
      (when (Thread/interrupted) (throw (InterruptedException.)))

      (when (zero? (mod (first batch) 1000))
        (log/trace :done (first batch)))

      (xt/submit-tx node
                    [(into [:put-docs table]
                           (map (fn [idx]
                                  {:xt/id idx}))
                           batch)]))))

(defmethod b/cli-flags :ingest-tx-overhead [_]
  [["-dc" "--doc-count DOCUMENT_COUNT" "Number of documents to ingest"
    :parse-fn parse-long
    :default 100000]

   ["-bs" "--batch-sizes BATCH_SIZES" "Batch sizes to use for ingestion, e.g. \"1000,100,10,1\" (currently supported: 1000, 100, 10, 1)"
    :parse-fn #(->> (s/split % #",") (map parse-long) (into #{}))
    :default #{1000 100 10 1}]

   ["-h" "--help"]])

(defn benchmark [{:keys [seed doc-count batch-sizes], :or {seed 0, doc-count 100000, batch-sizes #{1000 100 10 1}}}]
  {:title "Ingest batch vs individual"
   :seed seed
   :tasks (->> [{:t :call
                 :batch-size 1000
                 :stage :ingest-batch-1000
                 :f (fn [{:keys [node]}]
                      (with-open [conn (jdbc/get-connection node)]
                        (do-ingest conn :batched_1000 doc-count 1000)))}

                {:t :call
                 :batch-size 100
                 :stage :ingest-batch-100
                 :f (fn [{:keys [node]}]
                      (with-open [conn (jdbc/get-connection node)]
                        (do-ingest conn :batched_100 doc-count 100)))}

                {:t :call
                 :batch-size 10
                 :stage :ingest-batch-10
                 :f (fn [{:keys [node]}]
                      (with-open [conn (jdbc/get-connection node)]
                        (do-ingest conn :batched_10 doc-count 10)))}

                {:t :call
                 :batch-size 1
                 :stage :ingest-batch-1
                 :f (fn [{:keys [node]}]
                      (with-open [conn (jdbc/get-connection node)]
                        (do-ingest conn :batched_1 doc-count 1)))}]

               (filter (comp batch-sizes :batch-size)))})

(defmethod b/->benchmark :ingest-tx-overhead [_ {:keys [doc-count batch-sizes] :as opts}]
  (log/info {:doc-count doc-count :batch-sizes batch-sizes})
  (benchmark opts))


(comment
  (let [f (b/compile-benchmark (benchmark {:batch-sizes #{1000}}))]
    (with-open [^AutoCloseable
                node (case :xt-memory
                       :xt-memory (xtn/start-node)

                       :xt-local (let [path (util/->path "/tmp/xt-tx-overhead-bench")]
                                   (util/delete-dir path)
                                   (tu/->local-node {:node-dir path}))

                       :pg-conn (jdbc/get-connection {:dbtype "postgresql"
                                                      :dbname "postgres"
                                                      :user "postgres"
                                                      :password "postgres"}))]
      (f node))

    #_
    (f dev/node)))
