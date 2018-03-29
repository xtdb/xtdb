(ns juxt.rocks
  (:require [taoensso.nippy :as nippy]
            [juxt.byte-utils :refer :all]
            [clojure.set]
            [byte-streams :as bs]
            [gloss.core :as g]
            [gloss.io]
            [juxt.rocksdb :as rocksdb])
  (:import [org.rocksdb RocksDB Options]))

(def data-types {:long (g/compile-frame {:type :long, :v :int64})
                 :string (g/compile-frame {:type :string, :v (g/string :utf-8)})})

(def indices (g/compile-frame (g/enum :byte :eat :eid :aid :ident)))

(def db-keys {::key (g/compile-frame
                     (g/header
                      indices
                      {:eid  (g/compile-frame {:index :eat})
                       :eat (g/compile-frame (g/ordered-map :index :eat
                                                            :eid :int32
                                                            :aid :int32
                                                            :ts :int64))
                       :aid (g/compile-frame {:index :aid
                                              :aid :uint32})
                       :ident (g/compile-frame {:index :ident
                                                :ident :uint32}
                                               #(update % :ident hash-keyword)
                                               identity)}
                      :index))
              ::key-eid-frame (g/ordered-map :index indices :eid :int32)
              ::val-attribute-schema-frame (g/compile-frame
                                            (g/ordered-map
                                             :attr/type (apply g/enum :byte (keys data-types))
                                             :attr/ident (g/string :utf-8))
                                            (fn [m]
                                              (update m :attr/ident #(subs (str %) 1)))
                                            (fn [m]
                                              (update m :attr/ident keyword)))
              ::eat-val (g/compile-frame
                         (g/header
                          (g/compile-frame (apply g/enum :byte (keys data-types)))
                          data-types
                          :type))})

(defn- key->bytes [k-id & [m]]
  (->> m
       (gloss.io/encode (db-keys k-id))
       (bs/to-byte-array)))

(defn- decode [k-id v]
  (gloss.io/decode (get db-keys k-id) v))

(def o (Object.))

(defn next-entity-id "Return the next entity ID" [db]
  (locking o
    (let [key-entity-id (key->bytes ::key {:index :eid})]
      (.merge db key-entity-id (long->bytes 1))
      (bytes->long (.get db key-entity-id)))))

(defn transact-schema! "This might be merged with a future fn to
  transact any type of entity."
  [db {:keys [:attr/ident :attr/type]}]
  {:pre [ident type]}
  (let [aid (next-entity-id db)]
    ;; to go from k -> aid
    (.put db (key->bytes ::key {:index :ident :ident ident})
          (long->bytes aid))
    ;; to go from aid -> k
    (let [k (key->bytes ::key {:index :aid :aid aid})]
      (.put db k (key->bytes ::val-attribute-schema-frame
                             {:attr/type type
                              :attr/ident ident})))
    aid))

(defn- attr-schema [db ident]
  (if-let [[_ v] (rocksdb/get db (key->bytes ::key {:index :ident :ident ident}))]
    (bytes->long v)
    (throw (IllegalArgumentException. (str "Unrecognised schema attribute: " ident)))))

(defn attr-aid->schema [db aid]
  (if-let [[k v ] (rocksdb/get db (key->bytes ::key {:index :aid :aid aid}))]
    (decode ::val-attribute-schema-frame v)
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
             eid (or (and (= -1 eid) (next-entity-id db)) eid)]
         (.put db (key->bytes ::key {:index :eat
                                     :eid eid
                                     :aid aid
                                     :ts (.getTime ts)})
               (key->bytes ::eat-val {:type (:attr/type attr-schema)
                                      :v v})))))))

(defn -get-at
  ([db eid k] (-get-at db eid k (java.util.Date.)))
  ([db eid k ts]
   (let [aid (attr-schema db k)
         attr-schema (attr-aid->schema db aid)
         i (.newIterator db)
         k (key->bytes ::key {:index :eat
                              :eid eid
                              :aid aid
                              :ts (.getTime ts)})]
     (try
       (.seekForPrev i k)
       (when (and (.isValid i) (let [km (decode ::key (.key i))]
                                 (and (= eid (:eid km)) (= aid (:aid km)))))
         (:v (decode ::eat-val (.value i))))
       (finally
         (.close i))))))

(defn entity "Return an entity. Currently iterates through all keys of
  an entity."
  ([db eid]
   (entity db eid (java.util.Date.)))
  ([db eid ts]
   (into {}
         (for [[k v] (rocksdb/seek-and-iterate db (key->bytes ::key-eid-frame {:index :eat :eid eid}))
               :let [{:keys [eid aid]} (decode ::key k)
                     attr-schema (attr-aid->schema db aid)]]
           [(:attr/ident attr-schema) (:v (decode ::eat-val v))]))))

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
