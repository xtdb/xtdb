(ns crux.core
  (:require [taoensso.nippy :as nippy]
            [crux.byte-utils :refer :all]
            [crux.kv :as kv]
            [clojure.set]
            [byte-streams :as bs]
            [clj-time.core :as time]
            [clj-time.coerce :as c]
            [gloss.core :as g]
            [gloss.io]))

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
      (kv/merge! db key-entity-id (long->bytes 1))
      (bytes->long (kv/seek db key-entity-id)))))

(defn transact-schema! "This might be merged with a future fn to
  transact any type of entity."
  [db {:keys [:attr/ident :attr/type]}]
  {:pre [ident type]}
  (let [aid (next-entity-id db)]
    ;; to go from k -> aid
    (kv/store db (encode :key {:index :ident :ident ident})
              (encode :val/ident {:aid aid}))
    ;; to go from aid -> k
    (let [k (encode :key {:index :aid :aid aid})]
      (kv/store db k (encode :val/attr {:attr/type type
                                        :attr/ident ident})))
    aid))

(defn- attr-schema [db ident]
  (or (some->> {:index :ident :ident ident}
               (encode :key)
               (kv/seek db)
               (decode :val/ident)
               :aid)
      (throw (IllegalArgumentException. (str "Unrecognised schema attribute: " ident)))))

(defn attr-aid->schema [db aid]
  (if-let [v (kv/seek db (encode :key {:index :aid :aid aid}))]
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
         (kv/store db (encode :key {:index :eat
                                  :eid eid
                                  :aid aid
                                  :ts (.getTime ts)})
                 (encode :val/eat (if v {:type (:attr/type attr-schema) :v v} {:type :retracted}))))))))

(defn -get-at
  ([db eid k] (-get-at db eid k (java.util.Date.)))
  ([db eid k ts]
   (let [aid (if (keyword? k) (attr-schema db k) k)] ;; knarly
     (some->> (kv/seek-and-iterate db
                                   (encode :key {:index :eat :eid eid :aid aid :ts (.getTime ts)})
                                   (encode :key {:index :eat :eid eid :aid aid :ts (.getTime (java.util.Date. 0 0 0))}))
              first second (decode :val/eat) :v))))

(defn entity "Return an entity. Currently iterates through all keys of
  an entity."
  ([db eid]
   (entity db eid (java.util.Date.)))
  ([db eid at-ts]
   (reduce (fn [m [k v]]
             (let [{:keys [eid aid ts]} (decode :key k)
                   attr-schema (attr-aid->schema db aid)
                   ident (:attr/ident attr-schema)]
               (if (or (ident m)
                       (or (not at-ts) (<= ts (- max-timestamp (.getTime at-ts)))))
                 m
                 (assoc m ident (:v (decode :val/eat v))))))
           {}
           (kv/seek-and-iterate db
                                (encode :key/eat-prefix {:index :eat :eid eid})
                                (encode :key/eat-prefix {:index :eat :eid (inc eid)})))))

(defn- entity-ids
  "Sequence of all entities in the DB. If this approach sticks, it
  could be a performance gain to replace this with a dedicate EID
  index that could be lazy."
  [db]
  (->> (kv/seek-and-iterate db
                            (encode :key/index-prefix {:index :eat})
                            (encode :key/index-prefix {:index :eid}))
       (map (fn [[k _]] (decode :key k)))
       (map :eid)
       (map vector)
       (into #{})))

(defn- filter-attr [db at-ts results [term-e term-aid term-v]]
  (update results term-e (fn [results]
                           (->> (or results (entity-ids db))
                                (keep (fn [[eid bindings]]
                                        (let [v (-get-at db eid term-aid at-ts)]
                                          (when (and v (or (not term-v) (symbol? term-v) (= term-v v)))
                                            [eid (if (symbol? term-v)
                                                   (assoc bindings term-v v)
                                                   bindings)]))))))))

(defn- preprocess-terms [db terms]
  (for [[e a v] terms]
    [e (attr-schema db a) v]))

(defn q
  ([db terms]
   (q db terms (java.util.Date.)))
  ([db terms ts]
   (into #{} (->> terms
                  (preprocess-terms db)
                  (reduce (partial filter-attr db ts) nil)
                  (reduce (fn [results [term-e eids]]
                            (if (nil? results)
                              (map #(hash-map term-e (first %) :bindings (second %)) eids)
                              (for [m results
                                    [eid bindings] eids
                                    :let [intersected-bindings (clojure.set/intersection (set (keys bindings))
                                                                                         (set (keys (:bindings m))))]
                                    :when (= (select-keys bindings intersected-bindings)
                                             (select-keys (:bindings m) intersected-bindings))]
                                (assoc m term-e eid))))
                          nil)
                  (map #(dissoc % :bindings))))))
