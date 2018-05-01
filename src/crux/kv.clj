(ns crux.kv
  (:require [byte-streams :as bs]
            [crux.byte-utils :refer :all]
            crux.db
            [crux.kv-store :as kv-store]
            [gloss.core :as g]
            gloss.io))

(def max-timestamp (.getTime #inst "9999-12-30"))
(def frame-reverse-timestamp (g/compile-frame :int64 (partial - max-timestamp) identity))
(def frame-keyword (g/compile-frame (g/string :utf-8) #(subs (str %) 1) keyword))

(def data-types {:long (g/compile-frame {:type :long, :v :int64})
                 :string (g/compile-frame {:type :string, :v (g/string :utf-8)})
                 :keyword (g/compile-frame {:type :keyword, :v frame-keyword})
                 :retracted (g/compile-frame {:type :retracted})})

(def indices (g/compile-frame (g/enum :byte :eat :avt :eid :aid :ident)))

(def frames {:key (g/compile-frame
                   (g/header
                    indices
                    {:eid  (g/compile-frame {:index :eat})
                     :eat (g/compile-frame (g/ordered-map :index :eat
                                                          :eid :int32
                                                          :aid :int32
                                                          :ts frame-reverse-timestamp))
                     :avt (g/compile-frame (g/ordered-map :index :avt
                                                          :aid :int32
                                                          :v (g/compile-frame (g/finite-block 16)
                                                                              (comp md5 to-byte-array)
                                                                              identity)
                                                          :ts frame-reverse-timestamp
                                                          :eid :int32))
                     :aid (g/compile-frame {:index :aid
                                            :aid :uint32})
                     :ident (g/compile-frame {:index :ident
                                              :ident (g/compile-frame :uint32 hash-keyword identity)})}
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
                         :attr/ident frame-keyword))
             :val/ident (g/compile-frame {:aid :int32})})

(defn- encode [f m]
  (->> m (gloss.io/encode (frames f)) (bs/to-byte-array)))

(defn- decode [f v]
  (gloss.io/decode (get frames f) v))

(def o (Object.))

(defn next-entity-id "Return the next entity ID" [db]
  (locking o
    (let [key-entity-id (encode :key {:index :eid})]
      (kv-store/merge! db key-entity-id (long->bytes 1))
      (bytes->long (kv-store/seek db key-entity-id)))))

(defn transact-schema! "This might be merged with a future fn to
  transact any type of entity."
  [db {:keys [:attr/ident :attr/type]}]
  {:pre [ident type]}
  (let [aid (next-entity-id db)]
    ;; to go from k -> aid
    (kv-store/store db (encode :key {:index :ident :ident ident})
                    (encode :val/ident {:aid aid}))
    ;; to go from aid -> k
    (let [k (encode :key {:index :aid :aid aid})]
      (kv-store/store db k (encode :val/attr {:attr/type type
                                              :attr/ident ident})))
    aid))

(defn- attr-schema [db ident]
  (or (some->> {:index :ident :ident ident}
               (encode :key)
               (kv-store/seek db)
               (decode :val/ident)
               :aid)
      (throw (IllegalArgumentException. (str "Unrecognised schema attribute: " ident)))))

(defn attr-aid->schema [db aid]
  (if-let [v (kv-store/seek db (encode :key {:index :aid :aid aid}))]
    (decode :val/attr v)
    (throw (IllegalArgumentException. (str "Unrecognised attribute: " aid)))))

(defn- entity->txes [tx]
  (if (map? tx)
    (for [[k v] (dissoc tx ::id)]
      [(::id tx) k v])
    [tx]))

(defn -put
  "Put an attribute/value tuple against an entity ID. If the supplied
  entity ID is -1, then a new entity-id will be generated."
  ([db txs]
   (-put db txs (java.util.Date.)))
  ([db txs ^java.util.Date ts]
   (let [tmp-ids->ids (atom {})]
     (doseq [[eid k v] (mapcat entity->txes txs)]
       (let [aid (attr-schema db k)
             attr-schema (attr-aid->schema db aid)
             eid (or (and (pos? eid) eid)
                     (get @tmp-ids->ids eid)
                     (get (swap! tmp-ids->ids assoc eid (next-entity-id db)) eid))]
         (kv-store/store db (encode :key {:index :eat
                                          :eid eid
                                          :aid aid
                                          :ts (.getTime ts)})
                         (encode :val/eat (if v {:type (:attr/type attr-schema) :v v} {:type :retracted})))
         (when v
           (kv-store/store db (encode :key {:index :avt
                                            :aid aid
                                            :v v
                                            :ts (.getTime ts)
                                            :eid eid})
                           (long->bytes eid)))))
     @tmp-ids->ids)))

(defn -get-at
  ([db eid k] (-get-at db eid k (java.util.Date.)))
  ([db eid k ^java.util.Date ts]
   (let [aid (if (keyword? k) (attr-schema db k) k)] ;; knarly
     (some->> (kv-store/seek-and-iterate db
                                   (encode :key {:index :eat :eid eid :aid aid :ts (.getTime ts)})
                                   (encode :key {:index :eat :eid eid :aid aid :ts (.getTime (java.util.Date. 0 0 0))}))
              first second (decode :val/eat) :v))))

(defn entity "Return an entity. Currently iterates through all keys of
  an entity."
  ([db eid]
   (entity db eid (java.util.Date.)))
  ([db eid ^java.util.Date at-ts]
   (some->
    (reduce (fn [m [k v]]
              (let [{:keys [eid aid ts]} (decode :key k)
                    attr-schema (attr-aid->schema db aid)
                    ident (:attr/ident attr-schema)]
                (if (or (ident m)
                        (or (not at-ts) (<= ts (- max-timestamp (.getTime at-ts)))))
                  m
                  (assoc m ident (:v (decode :val/eat v))))))
            nil
            (kv-store/seek-and-iterate db
                                 (encode :key/eat-prefix {:index :eat :eid eid})
                                 (encode :key/eat-prefix {:index :eat :eid (inc eid)})))
    (assoc ::id eid))))

(defn- entity-ids
  "Sequence of all entities in the DB. If this approach sticks, it
  could be a performance gain to replace this with a dedicate EID
  index that could be lazy."
  [db]
  (->> (kv-store/seek-and-iterate db
                                  (encode :key/index-prefix {:index :eat})
                                  (encode :key/index-prefix {:index :avt}))
       (map (fn [[k _]] (decode :key k)))
       (map :eid)
       (into #{})))

;; can't do this.., unless you have a set of ids in there..
;; or somehow put the eid in the key

(defn- entity-ids-for-value [db aid v ^java.util.Date ts]
  (->> (kv-store/seek-and-iterate db
                                  (encode :key {:index :avt
                                                :aid aid
                                                :v v
                                                :ts (.getTime ts)
                                                :eid 0})
                                  (encode :key {:index :avt
                                                :aid aid
                                                :v v
                                                :ts (.getTime (java.util.Date. 0 0 0))
                                                :eid 0}))
       (map (fn [[k _]] (decode :key k)))
       (map :eid)
       (into #{})))

(defrecord KvEntity [eid kv ts]
  crux.db/Entity
  (attr-val [{:keys [eid]} attr]
    (let [aid (attr-schema kv attr)]
      (-get-at kv eid aid ts)))
  (raw-val [{:keys [eid]}]
    eid))

(defrecord KvDatasource [kv ts]
  crux.db/Datasource
  (entities [this]
    (map (fn [eid] (KvEntity. eid kv ts)) (entity-ids kv)))

  (entities-for-attribute-value [this a v]
    (let [aid (attr-schema kv a)]
      (map (fn [eid] (KvEntity. eid kv ts)) (entity-ids-for-value kv aid v ts)))))
