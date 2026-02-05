(ns xtdb.table-catalog
  (:require [clojure.set :as set]
            [integrant.core :as ig]
            [xtdb.trie :as trie]
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.time :as time])
  (:import (clojure.lang MapEntry)
           (com.google.protobuf ByteString)
           [java.time ZoneId ZoneOffset]
           [java.nio ByteBuffer]
           [java.nio.file Path]
           [java.util Map]
           (org.apache.arrow.vector.types.pojo Field Schema)
           (xtdb.block.proto TableBlock Partition)
           xtdb.catalog.BlockCatalog
           (xtdb.indexer LiveTable$FinishedBlock)
           (xtdb.storage BufferPool)
           xtdb.table.TableRef
           xtdb.trie.Trie
           (xtdb.util HyperLogLog)))

(def ^java.nio.file.Path block-table-metadata-path (util/->path "blocks"))

(defn ->table-block-dir ^java.nio.file.Path [^TableRef table]
  (-> (Trie/getTablePath table)
      (.resolve block-table-metadata-path)))

(defn ->table-block-metadata-obj-key ^java.nio.file.Path [^Path table-path block-idx]
  (-> table-path
      (.resolve block-table-metadata-path)
      (.resolve (format "b%s.binpb" (util/->lex-hex-string block-idx)))))

(defn ->table-block ^TableBlock [vec-types ^long row-count partitions hlls]
  (let [table-schema (Schema. (->> vec-types
                                   (mapv (fn [[col-name vec-type]]
                                           (types/->field col-name vec-type)))))]
    (-> (doto (TableBlock/newBuilder)
          (.setArrowSchema (ByteString/copyFrom (.serializeAsMessage table-schema)))
          (.setRowCount row-count)
          (.putAllColumnNameToHll ^Map (update-vals hlls (comp #(ByteString/copyFrom ^ByteBuffer %) ByteBuffer/.duplicate)))
          (.addAllPartitions partitions))
        (.build))))

(defn- local-date->instant [^java.time.LocalDate local-date]
  (.toInstant (.atStartOfDay local-date ZoneOffset/UTC)))

(defn ->partition [{:keys [^int level recency part tries max-block-idx]}]
  (let [builder  (doto (Partition/newBuilder)
                   (.setLevel level)
                   (.setPart (ByteString/copyFrom (byte-array part)))
                   (.addAllTries tries))]
    (when max-block-idx
      (.setMaxBlockIndex builder max-block-idx))
    (when recency
      (.setRecency builder (-> recency local-date->instant time/instant->micros)))
    (.build builder)))

(defn- instant->local-date ^java.time.LocalDate [^java.time.Instant instant]
  (.toLocalDate (.atZone instant (ZoneId/of "UTC"))))

(defn <-partition [^Partition partition]
  (cond-> {:level (.getLevel partition)
           :part (vec (.getPart partition))
           :tries (.getTriesList partition)}
    (.hasMaxBlockIndex partition) (assoc :max-block-idx (.getMaxBlockIndex partition))
    (.hasRecency partition) (assoc :recency (-> (.getRecency partition) time/micros->instant instant->local-date))))

(defn old-table-block? [^TableBlock table-block]
  (empty? (.getPartitionsList table-block)))

(defn trie-details->partitions [trie-details-list]
  (->> trie-details-list
       (group-by #(select-keys (trie/<-trie-details %) [:level :recency :part]))
       (map (fn [[partition trie-details]]
              (->partition (assoc partition :tries trie-details))))))

(defn old-table-block->new-table-block [^TableBlock table-block]
  (-> (doto (TableBlock/newBuilder)
        (.setArrowSchema (.getArrowSchema table-block))
        (.setRowCount (.getRowCount table-block))
        (.putAllColumnNameToHll (.getColumnNameToHllMap table-block))
        (.addAllPartitions (trie-details->partitions (.getTriesList table-block))))
      (.build)))

(defn <-table-block [table-block]
  (let [^TableBlock table-block (cond-> table-block
                                  (old-table-block? table-block) old-table-block->new-table-block)
        schema (Schema/deserializeMessage (ByteBuffer/wrap (.toByteArray (.getArrowSchema table-block))))]
    {:row-count (.getRowCount table-block)
     :fields (->> (for [^Field field (.getFields schema)]
                    (MapEntry/create (.getName field) field))
                  (into {}))
     :hlls (-> (.getColumnNameToHllMap table-block)
               (update-vals #(-> (.toByteArray ^ByteString %) HyperLogLog/toHLL)))
     :partitions (into [] (map <-partition) (.getPartitionsList table-block))}))

(defn- merge-vec-types [old-vec-types new-vec-types]
  (cond
    (nil? old-vec-types) (into {} new-vec-types)
    (nil? new-vec-types) (into {} old-vec-types)

    :else (->> (for [col-name (set/union (set (keys old-vec-types))
                                         (set (keys new-vec-types)))]
                 [col-name (types/merge-types (or (get old-vec-types col-name)
                                                  #xt/type :null)
                                              (or (get new-vec-types col-name)
                                                  #xt/type :null))])
               (into {}))))

(defn- merge-hlls [old-hlls new-hlls]
  (merge-with #(HyperLogLog/combine %1 %2) old-hlls new-hlls))

(defn- merge-tables [old-table {:keys [row-count vec-types hlls] :as delta-table}]
  (cond-> old-table
    delta-table (-> (update :row-count (fnil + 0) row-count)
                    (update :vec-types merge-vec-types vec-types)
                    (update :hlls merge-hlls hlls))))

(defn- new-tables-metadata [old-tables-metadata new-deltas-metadata]
  (let [tables (set (concat (keys old-tables-metadata) (keys new-deltas-metadata)))]
    (->> tables
         (map (fn [table]
                (MapEntry/create table
                                 (merge-tables (get old-tables-metadata table)
                                               (get new-deltas-metadata table)))))
         (into {}))))

(defn load-tables-to-metadata ^java.util.Map [^BufferPool buffer-pool, ^BlockCatalog block-cat]
  (when-let [block-idx (.getCurrentBlockIndex block-cat)]
    (let [tables (.getAllTables block-cat)]
      (->> (for [^TableRef table tables
                 :let [table-block-path (->table-block-metadata-obj-key (Trie/getTablePath table) block-idx)
                       {:keys [fields] :as tb} (-> (.getByteArray buffer-pool table-block-path)
                                                   (TableBlock/parseFrom)
                                                   (<-table-block))]]
             (MapEntry/create table (-> tb
                                        (assoc :vec-types (update-vals fields types/->type))
                                        (dissoc :fields))))
           (into {})))))

(deftype TableCatalog [^BufferPool buffer-pool, ^BlockCatalog block-cat
                       ^:volatile-mutable table->metadata]
  xtdb.catalog.TableCatalog
  (rowCount [_ table] (get-in table->metadata [table :row-count]))

  (getType [_ table col-name]
    (some-> (get-in table->metadata [table :vec-types])
            (get col-name)))

  (getTypes [_ table] (get-in table->metadata [table :vec-types]))
  (getTypes [_] (update-vals table->metadata :vec-types))

  (refresh [this]
    (set! (.table->metadata this)
          (-> (load-tables-to-metadata buffer-pool block-cat)
              (update-vals #(dissoc % :partitions)))))

  (finishBlock [this table-metadata table-partitions]
    (let [delta-table->metadata (->> table-metadata
                                     (map (fn [[table ^LiveTable$FinishedBlock fb]]
                                            [table {:vec-types (.getVecTypes fb)
                                                    :row-count (.getRowCount fb)
                                                    :hlls (.getHllDeltas fb)}]))
                                     (into {}))
          new-table->metadata (new-tables-metadata table->metadata delta-table->metadata)]

      (set! (.table->metadata this) new-table->metadata)

      (->> (for [[^TableRef table {:keys [row-count vec-types hlls]}] new-table->metadata
                 :let [partitions (get table-partitions table)
                       table-block (->table-block vec-types row-count partitions hlls)]]
             (MapEntry/create table table-block))
           (into {})))))

(defmethod ig/expand-key :xtdb/table-catalog [k _]
  {k {:buffer-pool (ig/ref :xtdb/buffer-pool)
      :block-cat (ig/ref :xtdb/block-catalog)}})

(defmethod ig/init-key :xtdb/table-catalog [_ {:keys [buffer-pool block-cat]}]
  (doto (TableCatalog. buffer-pool block-cat {})
    (.refresh)))
