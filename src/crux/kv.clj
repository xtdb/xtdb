(ns crux.kv
  (:require [crux.byte-utils :as bu :refer :all]
            [crux.codecs :as c]
            [crux.db]
            [crux.kv-store :as kv-store]
            [crux.kv-store-utils :as kvu]
            [taoensso.nippy :as nippy])
  (:import java.nio.ByteBuffer
           java.util.Date))

(set! *unchecked-math* :warn-on-boxed)

(def frame-index-enum
  "An enum byte used to identity a particular index."
  (c/enum :eat :avt :next-eid :ident-id :ident :meta))

(def frame-index-eat
  "The EAT index is used for providing rapid access to a value of an
  entity/attribute at a given point in time, used as the primary means
  to get an entity/attribute value, for direct access and for query
  purposes. This index uses reversed time."
  (c/frame :index frame-index-enum
           :eid :id
           :aid :id
           :ts :reverse-ts
           :tx-ts :reverse-ts))

(def frame-index-avt
  "The AVT index is used to find entities that have an attribute/value
  at a particular point in time, used for query purposes. This index
  uses reversed time."
  (c/frame :index frame-index-enum
           :aid :id
           :v :tagged
           :ts :reverse-ts
           :eid :id))

(def frame-index-ident-id
  "The eid index is used store information about the entity ID,
  including original external ID."
  (c/frame :index frame-index-enum
           :id :id))

(def frame-index-ident
  "The ident index is used to provide a mapping from an external ID to a
  numeric ID used for referencing in internal indices."
  (c/frame :index frame-index-enum
           :ident :md5))

(def frame-index-meta
  (c/frame :index frame-index-enum
           :key :keyword))

(def frame-index-selector
  "Partial key frame, used for selecting within a particular index."
  (c/frame :index frame-index-enum))

(def frame-index-eat-key-prefix
  "Partial key frame, used for iterating within all
  attributes/timestamps of a given entity."
  (c/frame :index frame-index-enum :eid :id))

(def frame-index-eat-key-prefix-business-time
  "Partial key frame, used for seeking attributes as of a business
  time."
  (c/frame :index frame-index-enum
           :eid :id
           :aid :id
           :ts :reverse-ts))

(defn next-entity-id "Return the next entity ID" [db]
  (locking db
    (let [key-entity-id (c/encode frame-index-selector {:index :next-eid})]
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
                    [[(c/encode frame-index-ident {:index :ident :ident ident})
                      (long->bytes id)]])
    ;; to go from id -> k
    (let [k (c/encode frame-index-ident-id {:index :ident-id :id id})]
      (kv-store/store db [[k (nippy/freeze ident)]]))
    id))

(defn- lookup-ident
  "Look up the ID for a given ident."
  [db ident]
  (some->> {:index :ident :ident ident}
           (c/encode frame-index-ident)
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
  (if-let [v (kvu/value db (c/encode frame-index-ident-id {:index :ident-id :id aid}))]
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
   (let [txs-to-put (transient [])]
     (loop [[[eid k v :as tx] & txs] (mapcat entity->txs txs)]
       (when tx
         (assert eid)
         (let [eid (ident->id! db eid)
               aid (attr-ident->aid! db k)]
           (conj! txs-to-put [(c/encode frame-index-eat {:index :eat
                                                         :eid eid
                                                         :aid aid
                                                         :ts ts
                                                         :tx-ts tx-ts})
                              (nippy/freeze v)])
           (doseq [v (if (coll? v) v [v]) :when v]
             (conj! txs-to-put [(c/encode frame-index-avt {:index :avt
                                                           :aid aid
                                                           :v v
                                                           :ts ts
                                                           :eid eid})
                                (long->bytes eid)])))
         (recur txs)))
     (kv-store/store db (persistent! txs-to-put)))))

(defn seek-first [db eid ident ^Date ts ^Date tx-ts]
  (when-let [aid (lookup-attr-ident-aid db ident)]
    (let [seek-k ^bytes (c/encode frame-index-eat-key-prefix-business-time
                                  {:index :eat :eid eid :aid aid :ts ts})]
      (when-let [[k v] (kvu/seek-first db
                                       #(zero? (bu/compare-bytes seek-k % (- (alength seek-k) 8)))
                                       #(or (not tx-ts)
                                            (.after tx-ts (:tx-ts (c/decode frame-index-eat %))))
                                       seek-k)]
        (nippy/thaw v)))))

(defn -get-at
  ([db eid ident]
   (-get-at db eid ident (Date.)))
  ([db eid ident ts]
   (-get-at db eid ident ts nil))
  ([db eid ident ^Date ts ^Date tx-ts]
   (let [eid (lookup-ident db eid)]
     (when eid
       (seek-first db eid ident ts tx-ts)))))

(defn entity "Return an entity. Currently iterates through all keys of
  an entity."
  ([db eid]
   (entity db eid (Date.)))
  ([db id ^Date at-ts]
   (let [eid (lookup-ident db id)
         k (c/encode frame-index-eat-key-prefix {:index :eat :eid eid})]
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

(def ^:private eat-index-prefix (c/encode frame-index-selector {:index :eat}))

(defn entity-ids
  "Sequence of all entities in the DB. If this approach sticks, it
  could be a performance gain to replace this with a dedicate EID
  index that could be lazy."
  [db]
  (->> (kvu/seek-and-iterate db (partial bu/bytes=? eat-index-prefix) eat-index-prefix)
       (into #{} (comp (map (fn [[k _]] (c/decode frame-index-eat k)))
                       (map :eid)))))

(defn- value-slice
  "Given an AVTE key, return the value bytes.
   We know, 17 bytes for index-type (1), aid (4), ts (8), eid (4)."
  [^bytes bs]
  (bu/byte-array-slice bs 5 (- (alength bs) 17)))

(defn entity-ids-for-range-value
  "Return a sequence of entities that are referenced as part of the AVT
  index, and match the given attribute/value/timestamp, given the
  specified range."
  [db ident min-v max-v ^Date ts]
  (let [aid (attr-ident->aid! db ident)
        seek-k (c/encode frame-index-avt {:index :avt
                                          :aid aid
                                          :v min-v
                                          :ts ts
                                          :eid 0})
        max-v-bytes (when max-v (c/encode (:tagged c/all-types) max-v))]
    (eduction
     (map (comp bytes->long second))
     (kvu/seek-and-iterate db
                           (fn [^bytes k]
                             (and (bu/bytes=? seek-k 5 k)
                                  (or (not max-v)
                                      (not (pos? ^long (bu/compare-bytes (value-slice k) max-v-bytes))))))
                           seek-k))))

(defn store-meta [db k v]
  (kv-store/store db
                  [[(c/encode frame-index-meta {:index :meta :key k})
                    (nippy/freeze v)]]))

(defn read-meta [db k]
  (some->> ^bytes (kvu/value db (c/encode frame-index-meta {:index :meta :key k}))
           nippy/thaw))
