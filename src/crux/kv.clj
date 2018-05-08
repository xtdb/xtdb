(ns crux.kv
  (:require [byte-streams :as bs]
            [crux.byte-utils :refer :all]
            crux.db
            [crux.kv-store :as kv-store]
            [crux.codecs :as c])
  (:import java.nio.ByteBuffer))

(def max-timestamp (.getTime #inst "9999-12-30"))

(def frame-data-type-enum
  "An enum byte used to identity a particular data-type. Can be used
  as a header to signify the follow bytes as conforming to a
  particular data-type."
  (c/compile-enum :long :string :keyword :retracted))

(def frame-index-enum
  "An enum byte used to identity a particular index."
  (c/compile-enum :eat :avt :eid :aid :ident))

(def frame-index-eat
  "The EAT index is used for providing rapid access to a value of an
  entity/attribute at a given point in time, used as the primary means
  to get an entity/attribute value, for direct access and for query
  purposes. This index uses reversed time."
  (c/compile-frame :index frame-index-enum
                   :eid :id
                   :aid :id
                   :ts :reverse-ts))

(def frame-value-eat
  "The frame of the value stored inside of the EAT index."
  (c/compile-header-frame
   [:type frame-data-type-enum]
   {:long (c/compile-frame :type frame-data-type-enum :v :int64)
    :string (c/compile-frame :type frame-data-type-enum :v :string)
    :keyword (c/compile-frame :type frame-data-type-enum :v :keyword)
    :retracted (c/compile-frame :type frame-data-type-enum)}))

(def frame-index-avt
  "The AVT index is used to find entities that have an attribute/value
  at a particular point in time, used for query purposes. This index
  uses reversed time."
  (c/compile-frame :index frame-index-enum
                   :aid :id
                   :v :md5
                   :ts :reverse-ts
                   :eid :id))

(def frame-index-aid
  "The AID index is used to provide a mapping from attribute ID to
  information about the attribute, including the attribute's keyword
  ident."
  (c/compile-frame :index frame-index-enum
                   :aid :id))

(def frame-value-aid
  "The frame of the value stored inside of the AID index."
  (c/compile-frame
   :crux.kv.attr/type frame-data-type-enum
   :crux.kv.attr/ident :keyword))

(def frame-index-attribute-ident
  "The attribute-ident index is used to provide a mapping from
  attribute keyword ident to the attribute ID."
  (c/compile-frame :index frame-index-enum
                   :ident :md5))

(def frame-index-eid
  "The EID index is used for generating entity IDs; to store the next
  entity ID to use. A merge operator can be applied to increment the
  value."
  (c/compile-frame :index frame-index-enum))

(def frame-index-key
  "Frame to use for building keys, that can access any value across
  all the indicies. The first byte is a header that identifies the
  index and corresponding key structure."
  (c/compile-header-frame
   [:index frame-index-enum]
   {:eid frame-index-eid
    :eat frame-index-eat
    :avt frame-index-avt
    :aid frame-index-aid
    :ident frame-index-attribute-ident}))

(def frame-index-key-prefix
  "Partial key frame, used for iterating within a particular index."
  (c/compile-frame :index frame-index-enum))

(def frame-index-eat-key-prefix
  "Partial key frame, used for iterating within all
  attributes/timestamps of a given entity."
  (c/compile-frame :index frame-index-enum :eid :id))

(defn- encode [frame m]
  (.array ^ByteBuffer (c/encode frame m)))

(def o (Object.))

(defn next-entity-id "Return the next entity ID" [db]
  (locking o
    (let [key-entity-id (encode frame-index-key {:index :eid})]
      (kv-store/merge! db key-entity-id (long->bytes 1))
      (bytes->long (kv-store/seek db key-entity-id)))))

(defn transact-schema! "This might be merged with a future fn to
  transact any type of entity."
  [db {:keys [:crux.kv.attr/ident :crux.kv.attr/type]}]
  {:pre [ident type]}
  (let [aid (next-entity-id db)]
    ;; to go from k -> aid
    (kv-store/store db
                    (encode frame-index-key {:index :ident :ident ident})
                    (long->bytes aid))
    ;; to go from aid -> k
    (let [k (encode frame-index-key {:index :aid :aid aid})]
      (kv-store/store db k (encode frame-value-aid {:crux.kv.attr/type type
                                                    :crux.kv.attr/ident ident})))
    aid))

(defn- attr-schema [db ident]
  (or (some->> {:index :ident :ident ident}
               (encode frame-index-key)
               (kv-store/seek db)
               bytes->long)
      (throw (IllegalArgumentException. (str "Unrecognised schema attribute: " ident)))))

(defn attr-aid->schema [db aid]
  (if-let [v (kv-store/seek db (encode frame-index-key {:index :aid :aid aid}))]
    (c/decode frame-value-aid v)
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
                         (encode frame-value-eat (if v {:type (:crux.kv.attr/type attr-schema) :v v} {:type :retracted})))
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
              first second (c/decode frame-value-eat) :v))))

(defn entity "Return an entity. Currently iterates through all keys of
  an entity."
  ([db eid]
   (entity db eid (java.util.Date.)))
  ([db eid ^java.util.Date at-ts]
   (some->
    (reduce (fn [m [k v]]
              (let [{:keys [eid aid ts]} (c/decode frame-index-key k)
                    attr-schema (attr-aid->schema db aid)
                    ident (:crux.kv.attr/ident attr-schema)]
                (if (or (ident m)
                        (or (not at-ts) (<= ts (- max-timestamp (.getTime at-ts)))))
                  m
                  (assoc m ident (:v (c/decode frame-value-eat v))))))
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
       (map (fn [[k _]] (c/decode frame-index-key k)))
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
       (map (comp bytes->long second))))

(defn attributes
  "Sequence of all attributes in the DB."
  [db]
  (->> (kv-store/seek-and-iterate-bounded db (encode frame-index-key-prefix {:index :aid}))
       (map (fn [[k v]]
              (let [attr (c/decode frame-value-aid v)
                    k (c/decode frame-index-key k)]
                [(:crux.kv.attr/ident attr)
                 (assoc attr :crux.kv.attr/id (:aid k))])))
       (into {})))

(defrecord KvDatasource [kv ts attributes]
  crux.db/Datasource
  (entities [this]
    (entity-ids kv))

  (entities-for-attribute-value [this a v]
    (let [aid (:crux.kv.attr/id (attributes a))]
      (entity-ids-for-value kv aid v ts)))

  (attr-val [this eid attr]
    (let [aid (:crux.kv.attr/id (attributes attr))]
      (-get-at kv eid aid ts))))
