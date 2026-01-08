(ns xtdb.datasets.tpch
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.util :as util])
  (:import clojure.lang.MapEntry
           (io.airlift.tpch TpchColumn TpchColumnType$Base TpchEntity TpchTable)
           [java.nio ByteBuffer]
           (java.time LocalDate)
           org.apache.arrow.adbc.core.BulkIngestMode
           org.apache.arrow.memory.BufferAllocator
           xtdb.api.Xtdb
           (xtdb.arrow Relation VectorType VectorWriter)
           xtdb.adbc.XtdbConnection))

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

(defn- cell-reader [^TpchColumn col]
  (condp = (.getBase (.getType col))
    TpchColumnType$Base/IDENTIFIER (let [col-part (str (str/replace (.getColumnName col) #".+_" "") "_")]
                                     (fn [^TpchEntity e]
                                       (-> (str col-part (.getIdentifier col e))
                                           util/->iid
                                           ByteBuffer/wrap
                                           util/byte-buffer->uuid)))
    TpchColumnType$Base/INTEGER (fn [^TpchEntity e]
                                  (long (.getInteger col e)))
    TpchColumnType$Base/VARCHAR (fn [^TpchEntity e]
                                  (.getString col e))
    TpchColumnType$Base/DOUBLE (fn [^TpchEntity e]
                                 (.getDouble col e))
    TpchColumnType$Base/DATE (fn [^TpchEntity e]
                               (LocalDate/ofEpochDay (.getDate col e)))))

(defn- doc->id [doc pk-cols]
  (if (= 1 (count pk-cols))
    (doc (first pk-cols))
    (let [eid (str/join "___" (map doc pk-cols))]
      eid)))

(defn- tpch-table->docs [^TpchTable table scale-factor]
  (let [cell-readers (mapv (fn [^TpchColumn col]
                             (let [k (keyword (.getColumnName col))
                                   cell-reader (cell-reader col)]
                               (fn [^TpchEntity e]
                                 (MapEntry/create k (cell-reader e)))))
                           (.getColumns table))
        table-name (keyword (.getTableName table))
        pk-cols (get table->pkey table-name)]
    (for [^TpchEntity e (.createGenerator table scale-factor 1 1)]
      (let [doc (into {} (map #(% e)) cell-readers)]
        (assoc doc :xt/id (doc->id doc pk-cols))))))

(defn submit-docs! [node scale-factor]
  (log/info "Transacting TPC-H tables...")
  (doseq [^TpchTable table (TpchTable/getTables)]
    (let [table-name (keyword (.getTableName table))
          doc-count (->> (tpch-table->docs table scale-factor)
                         (partition-all 1000)
                         (pmap (fn [batch]
                                 (xt/submit-tx node
                                               [(into [:put-docs table-name] batch)])
                                 (count batch)))
                         (apply +))]
      (log/debug "Transacted" doc-count (.getTableName table))))
  (log/info "Transacted TPC-H tables..."))

(defn- iid-getter [^TpchColumn col]
  (let [col-part (str (str/replace (.getColumnName col) #".+_" "") "_")]
    (fn [^TpchEntity e]
      (-> (str col-part (.getIdentifier col e))
          util/->iid
          ByteBuffer/wrap
          util/byte-buffer->uuid))))

(defn- cell-writer [^TpchColumn col]
  (condp = (.getBase (.getType col))
    TpchColumnType$Base/IDENTIFIER [#xt/type :uuid
                                    (let [get-iid (iid-getter col)]
                                      (fn [^VectorWriter out-col, ^TpchEntity e]
                                        (.writeObject out-col (get-iid e))))]
    TpchColumnType$Base/INTEGER [#xt/type :i64
                                 (fn [^VectorWriter out-col, ^TpchEntity e]
                                   (.writeLong out-col (.getInteger col e)))]
    TpchColumnType$Base/VARCHAR [#xt/type :utf8
                                 (fn [^VectorWriter out-col, ^TpchEntity e]
                                   (.writeObject out-col (.getString col e)))]
    TpchColumnType$Base/DOUBLE [#xt/type :f64
                                (fn [^VectorWriter out-col, ^TpchEntity e]
                                  (.writeDouble out-col (.getDouble col e)))]
    TpchColumnType$Base/DATE [#xt/type [:date :day]
                              (fn [^VectorWriter out-col, ^TpchEntity e]
                                (.writeInt out-col (.getDate col e)))]))

(defn- write-col! [^Relation rel, ^TpchColumn col, entities]
  (let [col-name (.getColumnName col)
        [^VectorType vec-type, write-cell!] (cell-writer col)
        out-col (.vectorFor rel col-name (.getArrowType vec-type) (.isNullable vec-type))]
    (doseq [^TpchEntity e entities]
      (write-cell! out-col e))))

(defn- write-pk-col! [^Relation rel, ^TpchTable table, entities]
  (let [out-col (.vectorFor rel "_id" #xt.arrow/type :uuid false)
        pk-cols (table->pkey (keyword (.getTableName table)))]
    (if (= 1 (count pk-cols))
      (let [pk-col (.getColumn table (name (first pk-cols)))
            get-iid (iid-getter pk-col)]
        (doseq [entity entities]
          (.writeObject out-col (get-iid entity))))

      (let [pk-readers (->> pk-cols
                            (mapv (comp cell-reader
                                        (fn [k] (.getColumn table (name k))))))]
        (doseq [entity entities]
          (.writeObject out-col (->> (map #(% entity) pk-readers)
                                     (str/join "___")
                                     util/->iid
                                     (ByteBuffer/wrap)
                                     util/byte-buffer->uuid)))))))

(defn- open-rel ^xtdb.arrow.Relation [^BufferAllocator al, ^TpchTable table, entities]
  (util/with-close-on-catch [rel (Relation. al)]
    (write-pk-col! rel table entities)

    (doseq [^TpchColumn col (.getColumns table)]
      (write-col! rel col entities))
    
    (.setRowCount rel (count entities))
    rel))

(defn submit-rels! [^Xtdb node scale-factor]
  (log/info "Transacting TPC-H tables...")
  (with-open [^XtdbConnection conn (.connect node)]
    (doseq [^TpchTable table (TpchTable/getTables)
            :let [table-name (.getTableName table)]]
      (let [doc-count (->> (.createGenerator table scale-factor 1 1)
                           (partition-all 1000)
                           (pmap (fn [batch]
                                   (with-open [stmt (.bulkIngest conn table-name BulkIngestMode/CREATE_APPEND)
                                               rel (open-rel (.getAllocator node) table batch)]
                                     (.bind stmt rel)
                                     (.getAffectedRows (.executeUpdate stmt)))))
                           (apply +))]
        (log/debug "Transacted" doc-count table-name))))

  (log/info "Transacted TPC-H tables..."))

(defn- tpch-table->dml [^TpchTable table]
  (format "INSERT INTO %s (%s) VALUES (%s)"
          (.getTableName table)
          (->> (cons "_id" (for [^TpchColumn col (.getColumns table)]
                             (.getColumnName col)))
               (str/join ", "))
          (->> (repeat (inc (count (.getColumns table))) "?")
               (str/join ", "))))

(defn- tpch-table->dml-params [^TpchTable table, scale-factor]
  (let [table-name (.getTableName table)
        cell-readers (mapv (fn [^TpchColumn col]
                             (let [k (keyword (.getColumnName col))
                                   cell-reader (cell-reader col)]
                               (fn [^TpchEntity e]
                                 (MapEntry/create k (cell-reader e)))))
                           (.getColumns table))
        pk-cols (get table->pkey (keyword table-name))]
    (for [^TpchEntity e (.createGenerator table scale-factor 1 1)
          :let [doc (map #(% e) cell-readers)]]
      (cons (doc->id (into {} doc) pk-cols)
            (vals doc)))))

(defn submit-dml! [node scale-factor]
  (log/debug "Transacting TPC-H tables...")
  (->> (TpchTable/getTables)
       (reduce (fn [_last-tx ^TpchTable table]
                 (let [dml (tpch-table->dml table)
                       [last-tx doc-count] (->> (tpch-table->dml-params table scale-factor)
                                                (partition-all 1000)
                                                (reduce (fn [[_!last-tx last-doc-count] param-batch]
                                                          [(xt/submit-tx node
                                                                         [(into [:sql dml] param-batch)])
                                                           (+ last-doc-count (count param-batch))])
                                                        [nil 0]))]
                   (log/debug "Transacted" doc-count (.getTableName table))
                   last-tx))
               nil)))

(defn submit-dml-jdbc!
  ([conn scale-factor]
   (submit-dml-jdbc! conn scale-factor nil))
  ([conn scale-factor table-names]
   (log/info "Transacting TPC-H tables...")
   (let [all-tables (TpchTable/getTables)
         table-name-set (when table-names (set table-names))
         tables-to-load (if table-name-set
                          (filter #(table-name-set (.getTableName ^TpchTable %)) all-tables)
                          all-tables)]
     (doseq [^TpchTable table tables-to-load]
       (let [dml (tpch-table->dml table)
             doc-count (->> (tpch-table->dml-params table scale-factor)
                            (partition-all 1000)
                            (transduce (map (fn [param-batch]
                                              (jdbc/with-transaction [tx conn]
                                                (jdbc/execute-batch! tx dml param-batch {}))
                                              (count param-batch)))
                                       + 0))]
         (log/debug "Transacted" doc-count (.getTableName table)))))))

(comment
  (with-open [conn (jdbc/get-connection {:dbname "xtdb"
                                         :host "localhost"
                                         :port 5432
                                         :classname "xtdb.jdbc.Driver"
                                         :dbtype "xtdb"})]
    (submit-dml-jdbc! conn 0.05)))
