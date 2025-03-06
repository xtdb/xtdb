(ns xtdb.table-catalog
  (:require [integrant.core :as ig]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util])
  (:import (clojure.lang MapEntry)
           (com.google.protobuf ByteString)
           [java.nio ByteBuffer]
           [java.nio.file Path]
           [java.util ArrayList]
           (org.apache.arrow.vector.types.pojo Field Schema)
           (xtdb BufferPool)
           (xtdb.block.proto TableBlock)
           xtdb.catalog.BlockCatalog))

(defprotocol PTableCatalog
  (finish-block! [table-cat block-idx delta-tables->metadata table->current-tries])
  (column-fields [table-cat table-name])
  (row-count [table-cat table-name])
  (column-field [table-cat talbe-name col-name])
  (all-column-fields [table-cat]))

(def ^java.nio.file.Path block-table-metadata-path (util/->path "blocks"))

(defn ->table-block-dir ^java.nio.file.Path [table-name]
  (-> (trie/table-name->table-path table-name)
      (.resolve block-table-metadata-path)))

(defn ->table-block-metadata-obj-key [^Path table-path block-idx]
  (.resolve (.resolve table-path block-table-metadata-path)
            (format "b%s.binpb" (util/->lex-hex-string block-idx))))

(defn write-table-block-data ^java.nio.ByteBuffer [^Schema table-schema ^long row-count
                                                   current-tries]
  (ByteBuffer/wrap (-> (doto (TableBlock/newBuilder)
                         (.setArrowSchema (ByteString/copyFrom (.serializeAsMessage table-schema)))
                         (.setRowCount row-count)
                         (.addAllCurrentTries current-tries))
                       (.build)
                       (.toByteArray))))

(defn <-table-block [^TableBlock table-block]
  (let [^Schema schema
        (Schema/deserializeMessage (ByteBuffer/wrap (.toByteArray (.getArrowSchema table-block))))]
    {:row-count (.getRowCount table-block)
     :fields (->> (for [^Field field (.getFields schema)]
                    (MapEntry/create (.getName field) field))
                  (into {}))
     :current-tries (into [] (.getCurrentTriesList table-block))}))

(defn- merge-fields [old-fields new-fields]
  (->> (merge-with types/merge-fields old-fields new-fields)
       (map (fn [[col-name field]] [col-name (types/field-with-name field col-name)]))
       (into {})))

(defn- merge-tables [old-table {:keys [row-count fields] :as delta-table}]
  (cond-> old-table
    delta-table (-> (update :row-count (fnil + 0) row-count)
                    (update :fields merge-fields fields))))

(defn- new-tables-metadata [old-tables-metadata new-deltas-metadata]
  (let [table-names (set (concat (keys old-tables-metadata) (keys new-deltas-metadata)))]
    (->> table-names
         (map (fn [table-name]
                (MapEntry/create table-name
                                 (merge-tables (get old-tables-metadata table-name)
                                               (get new-deltas-metadata table-name)))))
         (into {}))))

(defn load-tables-to-metadata ^java.util.Map [^BufferPool buffer-pool, ^BlockCatalog block-cat]
  (when-let [block-idx (.getCurrentBlockIndex block-cat)]
    (let [table-names (.getAllTableNames block-cat)]
      [block-idx
       (->> (for [table-name table-names
                  :let [table-block-path (->table-block-metadata-obj-key (trie/table-name->table-path table-name) block-idx)
                        table-block (TableBlock/parseFrom (.getByteArray buffer-pool table-block-path))]]
              (MapEntry/create table-name (<-table-block table-block)))
            (into {}))])))

(deftype TableCatalog [^BufferPool buffer-pool
                       ^:volatile-mutable ^long block-idx
                       ^:volatile-mutable table->metadata]
  PTableCatalog
  (finish-block! [this block-idx delta-table->metadata table->current-tries]
    (when (or (nil? (.block-idx this)) (< (.block-idx this) block-idx))
      (let [new-table->metadata (new-tables-metadata table->metadata delta-table->metadata)
            table-names (ArrayList.)]
        (doseq [[table-name {:keys [row-count fields]}] new-table->metadata]
          (let [current-tries (get table->current-tries table-name)
                fields (for [[col-name field] fields]
                         (types/field-with-name field col-name))
                table-block-path (->table-block-metadata-obj-key (trie/table-name->table-path table-name) block-idx)]
            (.add table-names table-name)
            (.putObject buffer-pool table-block-path
                        (write-table-block-data (Schema. fields) row-count
                                                (map (fn [{:keys [trie-key data-file-size trie-metadata]}]
                                                       (trie/->trie-details table-name trie-key data-file-size trie-metadata))
                                                     current-tries)))))
        (set! (.block-idx this) block-idx)
        (set! (.table->metadata this) new-table->metadata)
        (vec table-names))))

  (column-fields [_ table-name] (get-in table->metadata [table-name :fields]))
  (row-count [_ table-name] (get-in table->metadata [table-name :row-count]))
  (column-field [_ table-name col-name]
    (some-> (get-in table->metadata [table-name :fields])
            (get col-name (types/->field col-name #xt.arrow/type :null true))))
  (all-column-fields [_] (update-vals table->metadata :fields)))

(defmethod ig/prep-key :xtdb/table-catalog [_ _]
  {:buffer-pool (ig/ref :xtdb/buffer-pool)
   :block-cat (ig/ref :xtdb/block-catalog)})

(defmethod ig/init-key :xtdb/table-catalog [_ {:keys [buffer-pool block-cat]}]
  (let [[block-idx table->table-block] (load-tables-to-metadata buffer-pool block-cat)]
    (TableCatalog. buffer-pool (or block-idx -1)
                   (update-vals table->table-block #(dissoc % :current-tries)))))

(defn <-node [node]
  (util/component node :xtdb/table-catalog))
