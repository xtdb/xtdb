(ns crux.rocks
  (:require [taoensso.nippy :as nippy]
            [crux.byte-utils :refer :all]
            [clojure.set]
            [byte-streams :as bs]
            [clj-time.core :as time]
            [clj-time.coerce :as c]
            [gloss.core :as g]
            [gloss.io]
            [crux.rocksdb :as rocksdb])
  (:import [org.rocksdb RocksDB Options]))

(def max-timestamp (.getTime #inst "9999-12-30"))

(def data-types {:long (g/compile-frame {:type :long, :v :int64})
                 :string (g/compile-frame {:type :string, :v (g/string :utf-8)})
                 :retracted (g/compile-frame {:type :retracted})})

(def indices (g/compile-frame (g/enum :byte :eat :eid :aid :ident)))

(def frames {:key (g/compile-frame
                   (g/header
                    indices
                    {:eid  (g/compile-frame {:index :eat})
                     :eat (g/compile-frame (g/ordered-map :index :eat
                                                          :eid :int32
                                                          :aid :int32
                                                          :ts :int64)
                                           #(update % :ts (partial - max-timestamp))
                                           identity)
                     :aid (g/compile-frame {:index :aid
                                            :aid :uint32})
                     :ident (g/compile-frame {:index :ident
                                              :ident :uint32}
                                             #(update % :ident hash-keyword)
                                             identity)}
                    :index))
             :key/index-prefix (g/ordered-map :index indices)
             :key/eat-prefix (g/ordered-map :index indices :eid :int32)
             :val/eat (g/compile-frame
                       (g/header
                        (g/compile-frame (apply g/enum :byte (keys data-types)))
                        data-types
                        :type))
             :val/attr (g/compile-frame
                        (g/ordered-map
                         :attr/type (apply g/enum :byte (keys data-types))
                         :attr/ident (g/string :utf-8))
                        (fn [m]
                          (update m :attr/ident #(subs (str %) 1)))
                        (fn [m]
                          (update m :attr/ident keyword)))
             :val/ident (g/compile-frame {:aid :int32})})

(defn- encode [f m]
  (->> m (gloss.io/encode (frames f)) (bs/to-byte-array)))

(defn- decode [f v]
  (gloss.io/decode (get frames f) v))

(def o (Object.))

(defn next-entity-id "Return the next entity ID" [db]
  (locking o
    (let [key-entity-id (encode :key {:index :eid})]
      (.merge db key-entity-id (long->bytes 1))
      (bytes->long (.get db key-entity-id)))))

(defn transact-schema! "This might be merged with a future fn to
  transact any type of entity."
  [db {:keys [:attr/ident :attr/type]}]
  {:pre [ident type]}
  (let [aid (next-entity-id db)]
    ;; to go from k -> aid
    (.put db (encode :key {:index :ident :ident ident})
          (encode :val/ident {:aid aid}))
    ;; to go from aid -> k
    (let [k (encode :key {:index :aid :aid aid})]
      (.put db k (encode :val/attr {:attr/type type
                                    :attr/ident ident})))
    aid))

(defn- attr-schema [db ident]
  (or (some->> {:index :ident :ident ident}
               (encode :key)
               (rocksdb/get db)
               second
               (decode :val/ident)
               :aid)
      (throw (IllegalArgumentException. (str "Unrecognised schema attribute: " ident)))))

(defn attr-aid->schema [db aid]
  (if-let [[k v ] (rocksdb/get db (encode :key {:index :aid :aid aid}))]
    (decode :val/attr v)
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
         (.put db (encode :key {:index :eat
                                :eid eid
                                :aid aid
                                :ts (.getTime ts)})
               (encode :val/eat (if v {:type (:attr/type attr-schema) :v v} {:type :retracted}))))))))

(defn -get-at
  ([db eid k] (-get-at db eid k (java.util.Date.)))
  ([db eid k ts]
   (let [aid (attr-schema db k)
         attr-schema (attr-aid->schema db aid)
         k (encode :key {:index :eat
                         :eid eid
                         :aid aid
                         :ts (.getTime ts)})]
     (when-let [[_ v] (rocksdb/get db k (fn [i] (let [km (decode :key (.key i))]
                                                  (and (= eid (:eid km)) (= aid (:aid km))))))]
       (:v (decode :val/eat v))))))

(defn entity "Return an entity. Currently iterates through all keys of
  an entity."
  ([db eid]
   (entity db eid (java.util.Date.)))
  ([db eid ts]
   (reduce (fn [m [k v]]
             (let [{:keys [eid aid]} (decode :key k)
                   attr-schema (attr-aid->schema db aid)
                   ident (:attr/ident attr-schema)]
               (if (ident m) m (assoc m ident (:v (decode :val/eat v))))))
           {}
           (rocksdb/seek-and-iterate db
                                     (encode :key/eat-prefix {:index :eat :eid eid})
                                     (encode :key/eat-prefix {:index :eat :eid (inc eid)})))))

(defn- filter-attr [db eids [query-k query-v]]
  (into #{}
        (let [aid (attr-schema db query-k)]
          (for [[k v] (rocksdb/seek-and-iterate db
                                                (encode :key/index-prefix {:index :eat})
                                                (encode :key/index-prefix {:index :eid}))
                :let [{:keys [eid] :as k} (decode :key k)]
                :when (and (or (not eids) (contains? eids eid))
                           (= aid (:aid k))
                           (or (not query-v) (= query-v (:v (decode :val/eat v)))))]
            eid))))

(defn query
  "For now, uses AET for all cases which is inefficient. Also
  inefficient because there is no short circuiting or intelligent
  movement across the EAT index."
  [db q]
  (reduce (partial filter-attr db) nil q))

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
