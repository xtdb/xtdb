(ns crux.kv
  (:require [crux.byte-utils :refer :all]
            [crux.db]
            [crux.kv-store :as kv-store]
            [crux.byte-utils :as bu]
            [crux.codecs :as c]
            [clojure.edn :as edn])
  (:import [java.nio ByteBuffer]
           [java.util Date]))

(def frame-data-type-enum
  "An enum byte used to identity a particular data-type. Can be used
  as a header to signify the follow bytes as conforming to a
  particular data-type."
  (c/compile-enum :long :string :keyword :retracted))

(def frame-index-enum
  "An enum byte used to identity a particular index."
  (c/compile-enum :eat :avt :eid :aid :ident :meta))

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
   :crux.kv.attr/ident :keyword))

(def frame-index-attribute-ident
  "The attribute-ident index is used to provide a mapping from
  attribute keyword ident to the attribute ID."
  (c/compile-frame :index frame-index-enum
                   :ident :md5))

(def frame-index-meta
  (c/compile-frame :index frame-index-enum
                   :key :keyword))

(def frame-index-eid
  "The EID index is used for generating entity IDs; to store the next
  entity ID to use. A merge operator can be applied to increment the
  value."
  (c/compile-frame :index frame-index-enum))

(def frame-index-key-prefix
  "Partial key frame, used for iterating within a particular index."
  (c/compile-frame :index frame-index-enum))

(def frame-index-eat-key-prefix
  "Partial key frame, used for iterating within all
  attributes/timestamps of a given entity."
  (c/compile-frame :index frame-index-enum :eid :id))

(defn encode [frame m]
  (.array ^ByteBuffer (c/encode frame m)))

(def o (Object.))

(defn next-entity-id "Return the next entity ID" [db]
  (locking o
    (let [key-entity-id (encode frame-index-eid {:index :eid})]
      (kv-store/merge! db key-entity-id (long->bytes 1))
      (bytes->long (kv-store/value db key-entity-id)))))

(defn- val->data-type
  "Determine the supported data type for the supplied value."
  [v]
  (cond (nil? v)
        :retracted

        (keyword? v)
        :keyword

        (string? v)
        :string

        (number? v)
        :long))

(defn- transact-attr-ident!
  [db ident]
  {:pre [ident]}
  (let [aid (next-entity-id db)]
    ;; to go from k -> aid
    (kv-store/store db
                    (encode frame-index-attribute-ident {:index :ident :ident ident})
                    (long->bytes aid))
    ;; to go from aid -> k
    (let [k (encode frame-index-aid {:index :aid :aid aid})]
      (kv-store/store db k (encode frame-value-aid {:crux.kv.attr/ident ident})))
    aid))

(defn- attributes-at-rest
  "Sequence of all attributes in the DB."
  [db]
  (let [k (encode frame-index-key-prefix {:index :aid})]
    (->> (kv-store/seek-and-iterate db (partial bu/bytes=? k) k)
         (into {} (map (fn [[k v]]
                         (let [attr (c/decode frame-value-aid v)
                               k (c/decode frame-index-aid k)]
                           [(:crux.kv.attr/ident attr)
                            (:aid k)])))))))

(defn- attr-ident->aid!
  "Look up the attribute ID for a given ident. Create it if not
  present."
  [{:keys [attributes] :as db} ident]
  (if (nil? @attributes)
    (reset! attributes (attributes-at-rest db)))
  (or (get @attributes ident)
      (let [aid (or (some->> {:index :ident :ident ident}
                             (encode frame-index-attribute-ident)
                             (kv-store/value db)
                             bytes->long)
                    (transact-attr-ident! db ident))]
        (swap! attributes assoc ident aid)
        aid)))

(defn attr-aid->ident [db aid]
  (if-let [v (kv-store/value db (encode frame-index-aid {:index :aid :aid aid}))]
    (:crux.kv.attr/ident (c/decode frame-value-aid v))
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
   (-put db txs (Date.)))
  ([db txs ^Date ts]
   (let [tmp-ids->ids (atom {})]
     (doseq [[eid k v] (mapcat entity->txes txs)]
       (let [aid (attr-ident->aid! db k)
             eid (or (and (pos? eid) eid)
                     (get @tmp-ids->ids eid)
                     (get (swap! tmp-ids->ids assoc eid (next-entity-id db)) eid))
             datatype (val->data-type v)]
         (assert datatype "Could not discern data-type from supplied value")
         (kv-store/store db (encode frame-index-eat {:index :eat
                                                     :eid eid
                                                     :aid aid
                                                     :ts ts})
                         (encode frame-value-eat {:type datatype :v v}))
         (when v
           (kv-store/store db (encode frame-index-avt {:index :avt
                                                       :aid aid
                                                       :v v
                                                       :ts ts
                                                       :eid eid})
                           (long->bytes eid)))))
     @tmp-ids->ids)))

(defn -get-at
  ([db eid ident] (-get-at db eid ident (Date.)))
  ([db eid ident ^Date ts]
   (let [aid (attr-ident->aid! db ident)
         seek-k #^bytes (encode frame-index-eat {:index :eat :eid eid :aid aid :ts ts})]
     (when-let [[k v] (kv-store/seek db seek-k)]
       ;; Ensure just the key we want (minus time)
       (when (zero? (bu/compare-bytes seek-k k (- (alength seek-k) 8)))
         (:v (c/decode frame-value-eat v)))))))

(defn entity "Return an entity. Currently iterates through all keys of
  an entity."
  ([db eid]
   (entity db eid (Date.)))
  ([db eid ^Date at-ts]
   (let [k (encode frame-index-eat-key-prefix {:index :eat :eid eid})]
     (some->
      (reduce (fn [m [k v]]
                (let [{:keys [eid aid ^Date ts]} (c/decode frame-index-eat k)
                      ident (attr-aid->ident db aid)]
                  (if (or (ident m)
                          (or (not at-ts) (> (.getTime ts) (.getTime at-ts))))
                    m
                    (assoc m ident (:v (c/decode frame-value-eat v))))))
              nil
              (kv-store/seek-and-iterate db (partial bu/bytes=? k) k))
      (assoc ::id eid)))))

(def ^:private eat-index-prefix (encode frame-index-key-prefix {:index :eat}))

(defn entity-ids
  "Sequence of all entities in the DB. If this approach sticks, it
  could be a performance gain to replace this with a dedicate EID
  index that could be lazy."
  [db]
  (->> (kv-store/seek-and-iterate db (partial bu/bytes=? eat-index-prefix) eat-index-prefix)
       (into #{} (comp (map (fn [[k _]] (c/decode frame-index-eat k))) (map :eid)))))

(defn entity-ids-for-value [db ident v ^Date ts]
  (let [aid (attr-ident->aid! db ident)
        k #^bytes (encode frame-index-avt {:index :avt
                                           :aid aid
                                           :v v
                                           :ts ts
                                           :eid 0})]
    (eduction
     (map (comp bytes->long second))
     (kv-store/seek-and-iterate db
                                (partial bu/bytes=? k (- (alength k) 12))
                                k))))

(defn store-meta [db k v]
  (kv-store/store db
                  (encode frame-index-meta {:index :meta :key k})
                  (.getBytes (pr-str v))))

(defn get-meta [db k]
  (some->> ^bytes (kv-store/value db (encode frame-index-meta {:index :meta :key k}))
           String.
           edn/read-string))
