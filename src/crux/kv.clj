(ns crux.kv
  (:require [byte-streams :as bs]
            [crux.byte-utils :refer :all]
            crux.db
            [crux.kv-store :as kv-store]
            [gloss.core :as g]
            [gloss.io :refer [decode]]))

(def max-timestamp (.getTime #inst "9999-12-30"))
(def frame-reverse-timestamp (g/compile-frame :int64 (partial - max-timestamp) identity))
(def frame-keyword (g/compile-frame (g/string :utf-8) #(subs (str %) 1) keyword))
(def frame-md5 (g/compile-frame (g/finite-block 16) (comp md5 to-byte-array) identity))
(def frame-id (g/compile-frame :int32))

(def data-types {:long (g/compile-frame {:type :long, :v :int64})
                 :string (g/compile-frame {:type :string, :v (g/string :utf-8)})
                 :keyword (g/compile-frame {:type :keyword, :v frame-keyword})
                 :retracted (g/compile-frame {:type :retracted})})

(def frame-data-type-enum
  "An enum byte used to identity a particular data-type. Can be used
  as a header to signify the follow bytes as conforming to a
  particular data-type."
  (g/compile-frame (apply g/enum :byte (keys data-types))))

(def frame-index-enum
  "An enum byte used to identity a particular index."
  (g/compile-frame (g/enum :byte :eat :avt :eid :aid :ident)))

(def frame-index-eat
  "The EAT index is used for providing rapid access to a value of an
  entity/attribute at a given point in time, used as the primary means
  to get an entity/attribute value, for direct access and for query
  purposes. This index uses reversed time."
  (g/compile-frame (g/ordered-map :index :eat
                                  :eid frame-id
                                  :aid frame-id
                                  :ts frame-reverse-timestamp)))

(def frame-value-eat
  "The frame of the value stored inside of the EAT index."
  (g/compile-frame
   (g/header
    frame-data-type-enum
    data-types
    :type)))

(def frame-index-avt
  "The AVT index is used to find entities that have an attribute/value
  at a particular point in time, used for query purposes. This index
  uses reversed time."
  (g/compile-frame (g/ordered-map :index :avt
                                  :aid frame-id
                                  :v frame-md5
                                  :ts frame-reverse-timestamp
                                  :eid frame-id)))

(def frame-index-aid
  "The AID index is used to provide a mapping from attribute ID to
  information about the attribute, including the attribute's keyword
  ident."
  (g/compile-frame {:index :aid
                    :aid frame-id}))

(def frame-value-aid
  "The frame of the value stored inside of the AID index."
  (g/compile-frame
   (g/ordered-map
    :attr/type frame-data-type-enum
    :attr/ident frame-keyword)))

(def frame-index-attribute-ident
  "The attribute-ident index is used to provide a mapping from
  attribute keyword ident to the attribute ID."
  (g/compile-frame {:index :ident
                    :ident frame-md5}))

(def frame-index-eid
  "The EID index is used for generating entity IDs; to store the next
  entity ID to use. A merge operator can be applied to increment the
  value."
  (g/compile-frame {:index :eat}))

(def frame-index-key
  "Frame to use for building keys, that can access any value across
  all the indicies. The first byte is a header that identifies the
  index and corresponding key structure."
  (g/compile-frame
   (g/header
    frame-index-enum
    {:eid frame-index-eid
     :eat frame-index-eat
     :avt frame-index-avt
     :aid frame-index-aid
     :ident frame-index-attribute-ident}
    :index)))

(def frame-index-key-prefix
  "Partial key frame, used for iterating within a particular index."
  (g/ordered-map :index frame-index-enum))

(def frame-index-eat-key-prefix
  "Partial key frame, used for iterating within all
  attributes/timestamps of a given entity."
  (g/ordered-map :index frame-index-enum :eid frame-id))

(defn- encode [frame m]
  (->> m (gloss.io/encode frame) (bs/to-byte-array)))

(def o (Object.))

(defn next-entity-id "Return the next entity ID" [db]
  (locking o
    (let [key-entity-id (encode frame-index-key {:index :eid})]
      (kv-store/merge! db key-entity-id (long->bytes 1))
      (bytes->long (kv-store/seek db key-entity-id)))))

(defn transact-schema! "This might be merged with a future fn to
  transact any type of entity."
  [db {:keys [:attr/ident :attr/type]}]
  {:pre [ident type]}
  (let [aid (next-entity-id db)]
    ;; to go from k -> aid
    (kv-store/store db
                    (encode frame-index-key {:index :ident :ident ident})
                    (encode frame-id aid))
    ;; to go from aid -> k
    (let [k (encode frame-index-key {:index :aid :aid aid})]
      (kv-store/store db k (encode frame-value-aid {:attr/type type
                                                    :attr/ident ident})))
    aid))

(defn- attr-schema [db ident]
  (or (some->> {:index :ident :ident ident}
               (encode frame-index-key)
               (kv-store/seek db)
               (decode frame-id))
      (throw (IllegalArgumentException. (str "Unrecognised schema attribute: " ident)))))

(defn attr-aid->schema [db aid]
  (if-let [v (kv-store/seek db (encode frame-index-key {:index :aid :aid aid}))]
    (decode frame-value-aid v)
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
         (kv-store/store db (encode frame-index-key {:index :eat
                                                     :eid eid
                                                     :aid aid
                                                     :ts (.getTime ts)})
                         (encode frame-value-eat (if v {:type (:attr/type attr-schema) :v v} {:type :retracted})))
         (when v
           (kv-store/store db (encode frame-index-key {:index :avt
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
                                   (encode frame-index-key {:index :eat :eid eid :aid aid :ts (.getTime ts)})
                                   (encode frame-index-key {:index :eat :eid eid :aid aid :ts (.getTime (java.util.Date. 0 0 0))}))
              first second (decode frame-value-eat) :v))))

(defn entity "Return an entity. Currently iterates through all keys of
  an entity."
  ([db eid]
   (entity db eid (java.util.Date.)))
  ([db eid ^java.util.Date at-ts]
   (some->
    (reduce (fn [m [k v]]
              (let [{:keys [eid aid ts]} (decode frame-index-key k)
                    attr-schema (attr-aid->schema db aid)
                    ident (:attr/ident attr-schema)]
                (if (or (ident m)
                        (or (not at-ts) (<= ts (- max-timestamp (.getTime at-ts)))))
                  m
                  (assoc m ident (:v (decode frame-value-eat v))))))
            nil
            (kv-store/seek-and-iterate db
                                 (encode frame-index-eat-key-prefix {:index :eat :eid eid})
                                 (encode frame-index-eat-key-prefix {:index :eat :eid (inc eid)})))
    (assoc ::id eid))))

(defn- entity-ids
  "Sequence of all entities in the DB. If this approach sticks, it
  could be a performance gain to replace this with a dedicate EID
  index that could be lazy."
  [db]
  (->> (kv-store/seek-and-iterate db
                                  (encode frame-index-key-prefix {:index :eat})
                                  (encode frame-index-key-prefix {:index :avt}))
       (map (fn [[k _]] (decode frame-index-key k)))
       (map :eid)
       (into #{})))

(defn- entity-ids-for-value [db aid v ^java.util.Date ts]
  (->> (kv-store/seek-and-iterate db
                                  (encode frame-index-key {:index :avt
                                                           :aid aid
                                                           :v v
                                                           :ts (.getTime ts)
                                                           :eid 0})
                                  (encode frame-index-key {:index :avt
                                                           :aid aid
                                                           :v v
                                                           :ts (.getTime (java.util.Date. 0 0 0))
                                                           :eid 0}))
       (map (fn [[k _]] (decode frame-index-key k)))
       (map :eid)
       (into #{})))

(defn attributes
  "Sequence of all attributes in the DB."
  [db]
  (->> (kv-store/seek-and-iterate-bounded db (encode frame-index-key-prefix {:index :aid}))
       (map (fn [[k v]]
              (let [attr (decode frame-value-aid v)
                    k (decode frame-index-key k)]
                [(:attr/ident attr)
                 (assoc attr :attr/id (:aid k))])))
       (into {})))

(defrecord KvDatasource [kv ts attributes]
  crux.db/Datasource
  (entities [this]
    (entity-ids kv))

  (entities-for-attribute-value [this a v]
    (let [aid (:attr/id (attributes a))]
      (entity-ids-for-value kv aid v ts)))

  (attr-val [this eid attr]
    (let [aid (:attr/id (attributes attr))]
      (-get-at kv eid aid ts))))
