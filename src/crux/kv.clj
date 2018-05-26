(ns crux.kv
  (:require [crux.byte-utils :as bu :refer :all]
            [crux.codecs :as c]
            [crux.kv-store :as kv-store]
            [crux.kv-store-utils :as kvu]
            [taoensso.nippy :as nippy])
  (:import java.nio.ByteBuffer
           java.util.Date))

(set! *unchecked-math* :warn-on-boxed)

(def frame-index-enum
  "An enum byte used to identity a particular index."
  (c/compile-enum :eat :avt :next-eid :ident-id :ident :meta))

(def frame-index-eat
  "The EAT index is used for providing rapid access to a value of an
  entity/attribute at a given point in time, used as the primary means
  to get an entity/attribute value, for direct access and for query
  purposes. This index uses reversed time."
  (c/compile-frame :index frame-index-enum
                   :eid :id
                   :aid :id
                   :ts :reverse-ts
                   :tx-ts :reverse-ts))

(def frame-index-avt
  "The AVT index is used to find entities that have an attribute/value
  at a particular point in time, used for query purposes. This index
  uses reversed time."
  (c/compile-frame :index frame-index-enum
                   :aid :id
                   :v (c/variable-data-type :long :md5)
                   :ts :reverse-ts
                   :eid :id))

(def frame-index-ident-id
  "The eid index is used store information about the entity ID,
  including original external ID."
  (c/compile-frame :index frame-index-enum
                   :id :id))

(def frame-index-ident
  "The ident index is used to provide a mapping from an external ID to a
  numeric ID used for referencing in internal indices."
  (c/compile-frame :index frame-index-enum
                   :ident :md5))

(def frame-index-meta
  (c/compile-frame :index frame-index-enum
                   :key :keyword))

(def frame-index-selector
  "Partial key frame, used for selecting within a particular index."
  (c/compile-frame :index frame-index-enum))

(def frame-index-eat-key-prefix
  "Partial key frame, used for iterating within all
  attributes/timestamps of a given entity."
  (c/compile-frame :index frame-index-enum :eid :id))

(def frame-index-eat-key-prefix-business-time
  "Partial key frame, used for seeking attributes as of a business
  time."
  (c/compile-frame :index frame-index-enum
                   :eid :id
                   :aid :id
                   :ts :reverse-ts))

(defn encode [frame m]
  (.array ^ByteBuffer (c/encode frame m)))

(defn next-entity-id "Return the next entity ID" [db]
  (locking db
    (let [key-entity-id (encode frame-index-selector {:index :next-eid})]
      (kv-store/store
       db
       [[key-entity-id
         (bu/long->bytes
          (if-let [old-value (kvu/value db key-entity-id)]
            (inc (bu/bytes->long old-value))
            1))]])
      (bytes->long (kvu/value db key-entity-id)))))

(defn- transact-ident!
  "Transact the identifier, creating a bi-directional mapping to a newly
  generated internal identifer used for indexing purposes."
  [db ident]
  {:pre [ident]}
  (let [id (next-entity-id db)]
    ;; to go from k -> id
    (kv-store/store db
                    [[(encode frame-index-ident {:index :ident :ident ident})
                      (long->bytes id)]])
    ;; to go from id -> k
    (let [k (encode frame-index-ident-id {:index :ident-id :id id})]
      (kv-store/store db [[k (nippy/freeze ident)]]))
    id))

(defn- lookup-ident
  "Look up the ID for a given ident."
  [db ident]
  (some->> {:index :ident :ident ident}
           (encode frame-index-ident)
           (kvu/value db)
           bytes->long))

(defn- ident->id!
  "Look up the ID for a given ident. Create it if not present."
  [db ident]
  (or (lookup-ident db ident)
      (transact-ident! db ident)))

(defn- lookup-attr-ident-aid
  "Look up the attribute ID for a given ident."
  [{:keys [state] :as db} ident]
  (or (get-in @state [:attributes ident])
      (when-let [aid (lookup-ident db ident)]
        (swap! state assoc-in [:attributes ident] aid)
        aid)))

(defn- attr-ident->aid!
  "Look up the attribute ID for a given ident. Create it if not
  present."
  [{:keys [state] :as db} ident]
  (or (lookup-attr-ident-aid db ident)
      (let [aid (transact-ident! db ident)]
        (swap! state assoc-in [:attributes ident] aid)
        aid)))

(defn attr-aid->ident [db aid]
  (if-let [v (kvu/value db (encode frame-index-ident-id {:index :ident-id :id aid}))]
    (nippy/thaw v)
    (throw (IllegalArgumentException. (str "Unrecognised attribute: " aid)))))

(defn- entity->txs [tx]
  (if (map? tx)
    (for [[k v] (dissoc tx ::id)]
      [(::id tx) k v])
    [tx]))

(defn -put
  "Put an attribute/value tuple against an entity ID."
  ([db txs]
   (-put db txs (Date.)))
  ([db txs ts]
   (-put db txs ts ts))
  ([db txs ^Date ts ^Date tx-ts]
   (->> (mapcat entity->txs txs)
        (reduce
         (fn [txs-to-put [eid k v]]
           (assert eid)
           (let [eid (ident->id! db eid)
                 aid (attr-ident->aid! db k)]
             (cond-> txs-to-put
               true (conj! [(encode frame-index-eat {:index :eat
                                                     :eid eid
                                                     :aid aid
                                                     :ts ts
                                                     :tx-ts tx-ts})
                            (nippy/freeze v)])
               v (conj! [(encode frame-index-avt {:index :avt
                                                  :aid aid
                                                  :v [(if (integer? v) :long :md5) v]
                                                  :ts ts
                                                  :eid eid})
                         (long->bytes eid)]))))
         (transient []))
        (persistent!)
        (kv-store/store db))))

(defn -get-at
  ([db eid ident]
   (-get-at db eid ident (Date.)))
  ([db eid ident ts]
   (-get-at db eid ident ts nil))
  ([db eid ident ^Date ts ^Date tx-ts]
   (let [aid (lookup-attr-ident-aid db ident)]
     (when aid
       (let [eid (lookup-ident db eid)
             seek-k ^bytes (encode frame-index-eat-key-prefix-business-time
                                   {:index :eat :eid eid :aid aid :ts ts})]
         (when-let [[k v] (kvu/seek-first db
                                          #(zero? (bu/compare-bytes seek-k % (- (alength seek-k) 8)))
                                          #(or (not tx-ts)
                                               (.after tx-ts (:tx-ts (c/decode frame-index-eat %))))
                                          seek-k)]
           (nippy/thaw v)))))))

(defn entity "Return an entity. Currently iterates through all keys of
  an entity."
  ([db eid]
   (entity db eid (Date.)))
  ([db id ^Date at-ts]
   (let [eid (lookup-ident db id)
         k (encode frame-index-eat-key-prefix {:index :eat :eid eid})]
     (some->
      (reduce (fn [m [k v]]
                (let [{:keys [eid aid ^Date ts]} (c/decode frame-index-eat k)
                      ident (attr-aid->ident db aid)]
                  (if (or (ident m)
                          (or (not at-ts) (> (.getTime ts) (.getTime at-ts))))
                    m
                    (assoc m ident (nippy/thaw v)))))
              nil
              (kvu/seek-and-iterate db (partial bu/bytes=? k) k))
      (assoc ::id id)))))

(def ^:private eat-index-prefix (encode frame-index-selector {:index :eat}))

(defn entity-ids
  "Sequence of all entities in the DB. If this approach sticks, it
  could be a performance gain to replace this with a dedicate EID
  index that could be lazy."
  [db]
  (->> (kvu/seek-and-iterate db (partial bu/bytes=? eat-index-prefix) eat-index-prefix)
       (into #{} (comp (map (fn [[k _]] (c/decode frame-index-eat k)))
                       (map :eid)
                       (map (partial attr-aid->ident db))))))

(defn entity-ids-for-value
  "Return a sequence of entities that are referenced as part of the AVT
  index, and match the given attribute/value/timestamp."
  ([db ident v]
   (entity-ids-for-value db ident v (Date.)))
  ([db ident v ^Date ts]
   (let [aid (attr-ident->aid! db ident)
         k ^bytes (encode frame-index-avt {:index :avt
                                           :aid aid
                                           :v [(if (integer? v) :long :md5) v]
                                           :ts ts
                                           :eid 0})]
     (eduction
      (map (comp (partial attr-aid->ident db) bytes->long second))
      (kvu/seek-and-iterate db
                            (partial bu/bytes=? k (- (alength k) 12))
                            k)))))

(defn- iterate-within-range [db aid ts min-v max-v]
  (let [seek-k (encode frame-index-avt {:index :avt
                                        :aid aid
                                        :v [:long min-v]
                                        :ts ts
                                        :eid 0})]
    (kvu/seek-and-iterate db
                          (fn [^bytes k]
                            (and (bu/bytes=? seek-k 5 k)
                                 (let [v (:v (c/decode frame-index-avt k))]
                                   (and (or (not min-v) (>= v min-v))
                                        (or (not max-v)
                                            (<= v max-v))))))
                          seek-k)))

(defn entity-ids-for-range-value
  [db ident min-v max-v ^Date ts]
  (let [aid (attr-ident->aid! db ident)]
    (eduction
     (map (comp (partial attr-aid->ident db) bytes->long second))
     (into [] (iterate-within-range db aid ts (or min-v Long/MIN_VALUE) max-v)))))

(defn store-meta [db k v]
  (kv-store/store db
                  [[(encode frame-index-meta {:index :meta :key k})
                    (nippy/freeze v)]]))

(defn read-meta [db k]
  (some->> ^bytes (kvu/value db (encode frame-index-meta {:index :meta :key k}))
           nippy/thaw))
