(ns juxt.rocks
  (:require [taoensso.nippy :as nippy]
            [juxt.byte-utils :refer :all]
            [clojure.set]
            [byte-streams :as bs]
            [gloss.core :as g]
            [gloss.io]
            [juxt.rocksdb :as rocksdb])
  (:import [org.rocksdb RocksDB Options]))

;; Todo, combine the below into gloss also:
(def attr-types {::Long {:id 1
                         :->bytes long->bytes
                         :<-bytes bytes->long}
                 ::String {:id 2
                           :->bytes str->bytes
                           :<-bytes bytes->str}})

(def attr-types-by-id (into {} (map (juxt (comp :id val) val) attr-types)))

(def db-keys {::key-entity-id {:index 1
                               :codec (g/compile-frame {:index-id :int16})}
              ::key-eid-aid-ts-frame {:index 2
                                      ;; TODO revise this down, we don't need minus numbers, can halve the size:
                                      :codec (g/compile-frame (g/ordered-map :index-id :int32
                                                                             :eid :int64
                                                                             :aid :int64
                                                                             :ts :int64))}
              ::key-eid-frame {:index 2
                               :codec (g/ordered-map :index-id :int32
                                                     :eid :int64)}
              ::key-attribute-ident-frame {:index 3
                                           :codec (g/compile-frame {:index-id :int16
                                                                    :ident-hash :uint32}
                                                                   (fn [{:keys [ident] :as m}]
                                                                     (assoc m :ident-hash (hash (str (namespace ident) (name ident)))))
                                                                   identity)}
              ::key-index-aid->hash-frame {:index 4
                                           :codec (g/compile-frame {:index-id :int16
                                                                    :aid :uint32})}
              ::val-attribute-schema-frame {:codec (g/ordered-map
                                                    :attr/type :int16
                                                    :attr/ident (g/string :utf-8))}})

(defn- key->bytes [k-id & [m]]
  (->> (assoc m :index-id (get-in db-keys [k-id :index]))
       (gloss.io/encode (get-in db-keys [k-id :codec]))
       (bs/to-byte-array)))

(defn- decode [k-id v]
  (gloss.io/decode (get-in db-keys [k-id :codec]) v))

(def o (Object.))

(defn next-entity-id "Return the next entity ID" [db]
  (locking o
    (let [key-entity-id (key->bytes ::key-entity-id)]
      (.merge db key-entity-id (long->bytes 1))
      (bytes->long (.get db key-entity-id)))))

(defn transact-schema! "This might be merged with a future fn to
  transact any type of entity."
  [db {:keys [:attr/ident :attr/type]}]
  {:pre [ident type]}
  (let [aid (next-entity-id db)]
    ;; to go from k -> aid
    (.put db (key->bytes ::key-attribute-ident-frame {:ident ident})
          (long->bytes aid))
    ;; to go from aid -> k
    (let [k (key->bytes ::key-index-aid->hash-frame {:aid aid})]
      ;; TODO move ident hacking to pre and post in the db-keys schema
      (.put db k (let [stringified-k (if (namespace ident)
                                       (str (namespace ident) "/" (name ident))
                                       (name ident))]
                   (key->bytes ::val-attribute-schema-frame
                               {:attr/type ((attr-types type) :id) ;; todo use an enum
                                :attr/ident stringified-k}))))
    aid))

(defn- attr-schema [db ident]
  (if-let [[_ v] (rocksdb/get db (key->bytes ::key-attribute-ident-frame {:ident ident}))]
    (bytes->long v)
    (throw (IllegalArgumentException. (str "Unrecognised schema attribute: " ident)))))

(defn attr-aid->schema [db aid]
  (if-let [[k v ] (rocksdb/get db (key->bytes ::key-index-aid->hash-frame {:aid aid}))]
    (update (decode ::val-attribute-schema-frame v) :attr/ident keyword)
    (throw (IllegalArgumentException. (str "Unrecognised attribute: " aid)))))

(defn -put
  "Put an attribute/value tuple against an entity ID. If the supplied
  entity ID is -1, then a new entity-id will be generated."
  ([db txs]
   (-put db txs (java.util.Date.)))
  ([db txs ts]
   (let [txs (if (map? txs)
               (for [[k v] (dissoc txs ::id)]
                 [(::id txs) k v])
               txs)]
     (doseq [[eid k v] txs]
       (let [aid (attr-schema db k)
             attr-schema (attr-aid->schema db aid)
             attr-schema-def (get attr-types-by-id (:attr/type attr-schema))
             eid (or (and (= -1 eid) (next-entity-id db)) eid)]
         (.put db (key->bytes ::key-eid-aid-ts-frame {:eid eid
                                                      :aid aid
                                                      :ts (.getTime ts)})
               ((:->bytes attr-schema-def) v)))))))

(defn -get-at
  ([db eid k] (-get-at db eid k (java.util.Date.)))
  ([db eid k ts]
   (let [aid (attr-schema db k)
         attr-schema (attr-aid->schema db aid)
         i (.newIterator db)
         k (key->bytes ::key-eid-aid-ts-frame {:eid eid
                                               :aid aid
                                               :ts (.getTime ts)})]
     (try
       (.seekForPrev i k)
       (when (and (.isValid i) (= (take 20 k)
                                  (take 20 (.key i))))
         ((:<-bytes (get attr-types-by-id (:attr/type attr-schema))) (.value i)))
       (finally
         (.close i))))))

(defn entity "Return an entity. Currently iterates through all keys of
  an entity."
  ([db eid]
   (entity db eid (java.util.Date.)))
  ([db eid ts]
   (into {}
         (for [[k v] (rocksdb/seek-and-iterate db (key->bytes ::key-eid-frame {:eid eid}))
               :let [{:keys [eid aid]} (decode ::key-eid-aid-ts-frame k)
                     attr-schema (attr-aid->schema db aid)]]
           ;; Todo, pull this out into a fn
           [(:attr/ident attr-schema) ((:<-bytes (get attr-types-by-id (:attr/type attr-schema))) v)]))))

(defn all-keys [db]
  (let [i (.newIterator db)]
    (try
      (.seekToFirst i)
      (println "Keys in the DB:")
      (doseq [v (rocksdb/rocks-iterator->seq i nil)]
        (println v))
      (finally
        (.close i)))))

(defn- db-path [db-name]
  (str "/tmp/" (name db-name) ".db"))

(defn open-db [db-name]
  ;; Open database
  (RocksDB/loadLibrary)
  (let [opts (doto (Options.)
               (.setCreateIfMissing true)
               (.setMergeOperatorName "uint64add"))]
    (RocksDB/open opts (db-path db-name))))

(defn destroy-db [db-name]
  (org.rocksdb.RocksDB/destroyDB (db-path db-name) (org.rocksdb.Options.)))

(comment
  (def c (open-db "repldb"))
  (.close c)
  ;; Print all keys:
  (all-keys db))
