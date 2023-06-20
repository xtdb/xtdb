(ns xtdb.datasets.tpch
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [cognitect.transit :as t]
            [xtdb.api :as xt]
            [xtdb.transit :as xt.t]
            [xtdb.util :as util])
  (:import clojure.lang.MapEntry
           (io.airlift.tpch TpchColumn TpchColumnType$Base TpchEntity TpchTable)
           (java.io File OutputStream)
           java.security.MessageDigest
           (java.time LocalDate)
           (java.util Arrays)))

;; 0.05 = 7500 customers, 75000 orders, 299814 lineitems, 10000 part, 40000 partsupp, 500 supplier, 25 nation, 5 region

(def ^:private table->pkey
  {:part [:p_partkey]
   :supplier [:s_suppkey]
   :partsupp [:ps_partkey :ps_suppkey]
   :customer [:c_custkey]
   :lineitem [:l_orderkey :l_linenumber]
   :orders [:o_orderkey]
   :nation [:n_nationkey]
   :region [:r_regionkey]})

(defn- ->cell-reader [^TpchColumn col]
  (comp (let [k (keyword (.getColumnName col))]
          (fn ->map-entry [v]
            (MapEntry/create k v)))

        (condp = (.getBase (.getType col))
          TpchColumnType$Base/IDENTIFIER (let [col-part (str (str/replace (.getColumnName col) #".+_" "") "_")]
                                           (fn [^TpchEntity e]
                                             (str col-part (.getIdentifier col e))))
          TpchColumnType$Base/INTEGER (fn [^TpchEntity e]
                                        (long (.getInteger col e)))
          TpchColumnType$Base/VARCHAR (fn [^TpchEntity e]
                                        (.getString col e))
          TpchColumnType$Base/DOUBLE (fn [^TpchEntity e]
                                       (.getDouble col e))
          TpchColumnType$Base/DATE (fn [^TpchEntity e]
                                     (LocalDate/ofEpochDay (.getDate col e))))))

(def ^:private ^java.security.MessageDigest msg-digest
  (MessageDigest/getInstance "SHA-256"))

(defn- ->iid ^bytes [^String eid]
  (-> (.digest msg-digest (.getBytes eid))
      (Arrays/copyOfRange 0 16)))

(defn- tpch-table->docs [^TpchTable table scale-factor]
  (let [cell-readers (mapv ->cell-reader (.getColumns table))
        table-name (keyword (.getTableName table))
        pk-cols (get table->pkey table-name)]
    (for [^TpchEntity e (.createGenerator table scale-factor 1 1)]
      (let [doc (into {} (map #(% e)) cell-readers)
            eid (str/join "___" (map doc pk-cols))]
        (-> (assoc doc :xt/id eid)
            (with-meta {:table table-name, :iid (->iid eid)}))))))

(defn submit-docs! [tx-producer scale-factor]
  (log/debug "Transacting TPC-H tables...")
  (->> (TpchTable/getTables)
       (reduce (fn [_last-tx ^TpchTable t]
                 (let [[!last-tx doc-count] (->> (tpch-table->docs t scale-factor)
                                                 (partition-all 1000)
                                                 (reduce (fn [[_!last-tx last-doc-count] batch]
                                                           [(xt/submit-tx& tx-producer
                                                                           (vec (for [doc batch]
                                                                                  [:put (:table (meta doc)) doc])))
                                                            (+ last-doc-count (count batch))])
                                                         [nil 0]))]
                   (log/debug "Transacted" doc-count (.getTableName t))
                   @!last-tx))
               nil)))

(defn- tpch-table->dml [^TpchTable table]
  (format "INSERT INTO %s (%s) VALUES (%s)"
          (.getTableName table)
          (->> (cons "xt$id" (for [^TpchColumn col (.getColumns table)]
                               (.getColumnName col)))
               (str/join ", "))
          (->> (repeat (inc (count (.getColumns table))) "?")
               (str/join ", "))))

(defn- tpch-table->dml-params [^TpchTable table, scale-factor]
  (let [table-name (.getTableName table)
        cell-readers (mapv ->cell-reader (.getColumns table))
        pk-cols (get table->pkey (keyword table-name))]
    (for [^TpchEntity e (.createGenerator table scale-factor 1 1)
          :let [doc (map #(% e) cell-readers)]]
      (cons (->> (mapv (into {} doc) pk-cols)
                 (str/join "___"))
            (vals doc)))))

(defn submit-dml! [tx-producer scale-factor]
  (log/debug "Transacting TPC-H tables...")
  (->> (TpchTable/getTables)
       (reduce (fn [_last-tx ^TpchTable table]
                 (let [dml (tpch-table->dml table)
                       [!last-tx doc-count] (->> (tpch-table->dml-params table scale-factor)
                                                 (partition-all 1000)
                                                 (reduce (fn [[_!last-tx last-doc-count] param-batch]
                                                           [(xt/submit-tx& tx-producer
                                                                           [[:sql-batch (into [dml] param-batch)]])
                                                            (+ last-doc-count (count param-batch))])
                                                         [nil 0]))]
                   (log/debug "Transacted" doc-count (.getTableName table))
                   @!last-tx))
               nil)))

(defn dump-tpch-log-files [^File root-dir {:keys [scale-factor tx-size seg-row-limit]
                                           :or {scale-factor 0.01
                                                tx-size 200
                                                seg-row-limit (bit-shift-left 1 20)}}]
  (let [!os (volatile! nil)]
    (try
      (->> (TpchTable/getTables)
           (transduce (comp (mapcat #(tpch-table->docs % scale-factor))
                            (map-indexed (fn [row-id doc]
                                           (let [{:keys [table iid]} (meta doc)]
                                             {:iid iid, :table table, :row-id row-id, :doc doc})))
                            (partition-all tx-size)
                            (map-indexed (fn [tx-id rows]
                                           {:tx-id tx-id
                                            :system-time (-> (util/->zdt #inst "2020") (.plusDays tx-id) util/->instant)
                                            :rows rows})))
                      (completing
                       (fn
                         ([] {:seg-row-count 0})
                         ([acc tx]
                          (let [{:keys [^long seg-row-count wtr]}
                                (loop [{:keys [^long seg-row-count] :as acc} acc]
                                  (cond
                                    (>= seg-row-count seg-row-limit) (do
                                                                       (some-> @!os util/close)
                                                                       (vreset! !os nil)
                                                                       (recur {:seg-row-count 0}))

                                    (nil? @!os) (let [{first-tx-id :tx-id, [{first-row-id :row-id}] :rows} tx
                                                      os (io/output-stream
                                                          (doto (io/file root-dir "input" (format "seg-r%s-tx%s.transit.json"
                                                                                                  (util/->lex-hex-string first-row-id)
                                                                                                  (util/->lex-hex-string first-tx-id)))
                                                            (io/make-parents)))]
                                                  (vreset! !os os)
                                                  (recur {:seg-row-count 0, :wtr (t/writer os :json {:handlers xt.t/tj-write-handlers}) }))
                                    :else acc))]

                            (when (Thread/interrupted)
                              (throw (InterruptedException.)))

                            (t/write wtr tx)
                            (.write ^OutputStream @!os (int \newline))

                            {:seg-row-count (+ seg-row-count (count (:rows tx))), :wtr wtr}))))))

      nil

      (finally
        (some-> @!os util/close)))))

(comment
  (dump-tpch-log-files (io/file "/home/james/tmp/idx-poc/tpch-01")
                       {:scale-factor 0.01}))
