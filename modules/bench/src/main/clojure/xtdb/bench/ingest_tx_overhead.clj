(ns xtdb.bench.ingest-tx-overhead
  (:require [clojure.string :as s]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.node :as xtn]
            [xtdb.protocols :as xtp]
            [xtdb.test-util :as tu]
            [xtdb.util :as util])
  (:import [java.lang AutoCloseable]
           [java.sql Connection]
           [xtdb.api Xtdb]
           [xtdb.protocols PNode]))

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

(defprotocol DoIngestSkipPGwire
  (do-ingest-without-pgwire [this table doc-count per-batch]))

(extend-protocol DoIngestSkipPGwire
  PNode
  (do-ingest-without-pgwire [node table ^long doc-count ^long per-batch]
    (doseq [batch (partition-all per-batch (range doc-count))]
      (when (Thread/interrupted) (throw (InterruptedException.)))

      (when (zero? (mod (first batch) 1000))
        (log/trace :done (first batch)))

      (xtp/submit-tx node
                     [(into [:put-docs table]
                            (map (fn [idx]
                                   {:xt/id idx}))
                            batch)]
                     {:default-db "xtdb"}))))


(defmethod b/cli-flags :ingest-tx-overhead [_]
  [["-dc" "--doc-count DOCUMENT_COUNT" "Number of documents to ingest"
    :parse-fn parse-long
    :default 100000]

   ["-bs" "--batch-sizes BATCH_SIZES" "Batch sizes to use for ingestion, e.g. \"1000,100,10,1\" (currently supported: 1000, 100, 10, 1)"
    :parse-fn #(->> (s/split % #",") (map parse-long) (into #{}))
    :default #{1000 100 10 1}]

   ["-h" "--help"]])

(defn benchmark [{:keys [seed doc-count batch-sizes ingest-fn],
                  :or {ingest-fn do-ingest seed 0, doc-count 100000, batch-sizes #{1000 100 10 1}}}]
  (log/info {:doc-count doc-count :batch-sizes batch-sizes})

  {:title "Ingest batch vs individual"
   :seed seed
   :parameters {:doc-count doc-count :batch-sizes (sort > batch-sizes)}
   :tasks (->> [{:t :call
                 :batch-size 1000
                 :stage :ingest-batch-1000
                 :f (fn [{:keys [node]}]
                      (ingest-fn node :batched_1000 doc-count 1000))}

                {:t :call
                 :batch-size 100
                 :stage :ingest-batch-100
                 :f (fn [{:keys [node]}]
                      (ingest-fn node :batched_100 doc-count 100))}

                {:t :call
                 :batch-size 10
                 :stage :ingest-batch-10
                 :f (fn [{:keys [node]}]
                      (ingest-fn node :batched_10 doc-count 10))}

                {:t :call
                 :batch-size 1
                 :stage :ingest-batch-1
                 :f (fn [{:keys [node]}]
                      (ingest-fn node :batched_1 doc-count 1))}]

               (filter (comp batch-sizes :batch-size)))})

(defmethod b/->benchmark :ingest-tx-overhead [_ {:keys [doc-count batch-sizes] :as opts}]
  (log/info {:doc-count doc-count :batch-sizes batch-sizes})
  (benchmark opts))

(comment
  ;; xt-memory - going through pg-wire, new connection every submit
  ;; xt-conn - going through pg-wire, one connection
  ;; xt-direct - using submit-tx directly on the node
  ;; xt-local - same as xt-memory but backed by disk
  ;; pg-conn - talking to real postgres
  (let [system-type :xt-direct
        ingest-fn (case system-type
                    (:xt-conn :xt-memory :xt-local :pg-conn) do-ingest
                    :xt-direct do-ingest-without-pgwire)
        f (b/compile-benchmark (benchmark {:batch-sizes #{100} :ingest-fn ingest-fn}))]
    (with-open [^AutoCloseable
                node (case system-type
                       (:xt-conn :xt-memory :xt-direct) (xtn/start-node)

                       :xt-local (let [path (util/->path "/tmp/xt-tx-overhead-bench")]
                                   (util/delete-dir path)
                                   (tu/->local-node {:node-dir path}))

                       :pg-conn (jdbc/get-connection {:dbtype "postgresql"
                                                      :dbname "postgres"
                                                      :user "postgres"
                                                      :password "postgres"}))]

      (case system-type
        (:xt-direct :xt-memory :xt-local :pg-conn)
        (f node)
        :xt-conn
        (with-open [conn (jdbc/get-connection node)]
          (f conn))))

    #_
    (f dev/node)))
