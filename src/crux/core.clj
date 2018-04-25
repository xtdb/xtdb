(ns crux.core
  (:require [byte-streams :as bs]
            clojure.set
            [crux.byte-utils :refer :all]
            [crux.kv :as kv]
            [gloss.core :as g]
            [clojure.spec.alpha :as s]
            gloss.io))

(def max-timestamp (.getTime #inst "9999-12-30"))

(def data-types {:long (g/compile-frame {:type :long, :v :int64})
                 :string (g/compile-frame {:type :string, :v (g/string :utf-8)})
                 :keyword (g/compile-frame {:type :keyword, :v (g/string :utf-8)}
                                           #(update % :v name)
                                           #(update % :v keyword))
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
  ([db txs ts]
   (let [tmp-ids->ids (atom {})]
     (doseq [[eid k v] (mapcat entity->txes txs)]
       (let [aid (attr-schema db k)
             attr-schema (attr-aid->schema db aid)
             eid (or (and (pos? eid) eid)
                     (get @tmp-ids->ids eid)
                     (get (swap! tmp-ids->ids assoc eid (next-entity-id db)) eid))]
         (kv/store db (encode :key {:index :eat
                                    :eid eid
                                    :aid aid
                                    :ts (.getTime ts)})
                   (encode :val/eat (if v {:type (:attr/type attr-schema) :v v} {:type :retracted})))))
     @tmp-ids->ids)))

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
            (kv/seek-and-iterate db
                                 (encode :key/eat-prefix {:index :eat :eid eid})
                                 (encode :key/eat-prefix {:index :eat :eid (inc eid)})))
    (assoc ::id eid))))

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
       (into #{})))

;; --------------
;; Query handling

(defn- expression-spec [sym spec]
  (s/and seq?
         #(= sym (first %))
         (s/conformer second)
         spec))

(s/def ::pred-fn (s/and symbol?
                        (s/conformer #(some-> % resolve var-get))
                        fn?))
(s/def ::find (s/coll-of symbol? :kind vector?))
(s/def ::rule (s/coll-of #(or (keyword? %)
                              (symbol? %)
                              (string? %))
                         :kind vector?))
(s/def ::term (s/or :term ::rule
                    :not (expression-spec 'not ::rule)
                    :or (expression-spec 'or ::where)
                    :not-join (s/cat :pred #{'not-join}
                                     :bindings (s/coll-of symbol? :kind vector?)
                                     :terms ::where)
                    :pred (s/cat ::pred-fn ::pred-fn
                                 ::args (s/* any?))))
(s/def ::where (s/coll-of ::term :kind vector?))
(s/def ::query (s/keys :req-un [::find ::where]))

(defn- value-matches? [[term-e term-a term-v] result]
  (when-let [v (result term-a)]
    (and v (or (not term-v)
               (and (symbol? term-v) (= (result term-v) v))
               (= term-v v)))))

(defn- apply-bindings [db at-ts [term-e term-a term-v] result]
  (if term-a
    (let [aid (attr-schema db term-a)
          v (-get-at db (result term-e) aid at-ts)
          result (assoc result term-a v)]
      (if (and (symbol? term-v) (not (result term-v)))
        (assoc result term-v v)
        result))
    result))

(defn- query-plan->results
  "Reduce over the query plan, unifying the results across each
  stage."
  [db at-ts plan]
  (reduce (fn [results [e bindings pred-f]]
            (let [results
                  (cond (not results)
                        (map #(hash-map e %) (entity-ids db))

                        (get (first results) e)
                        results

                        :else
                        (for [result results eid (entity-ids db)]
                          (assoc result e eid)))]
              (->> results
                   (map (partial apply-bindings db at-ts bindings))
                   (filter (partial pred-f db at-ts))
                   (into #{}))))
          nil plan))

(defn- query-terms->plan
  "Converts a sequence of query terms into a sequence of executable
  query stages."
  [terms]
  (for [[op t] terms]
    (condp = op
      :term
      [(first t)
       t
       (fn [_ _ result] (value-matches? t result))]

      :not
      [(first t)
       t
       (fn [_ _ result] (not (value-matches? t result)))]

      :or
      (let [e (-> t first second first)]
        [e
         (-> t first second)
         (fn [_ _ result]
           (when (some (fn [[_ term]]
                         (value-matches? term result))
                       t)
             result))])

      :not-join
      (let [e (-> t :bindings first)]
        [e
         nil
         (let [query-plan (query-terms->plan (:terms t))
               or-results (atom nil)]
           (fn [db at-ts result]
             (let [or-results (or @or-results (reset! or-results (query-plan->results db at-ts query-plan)))]
               (when-not (some #(= (get result e) (get % e)) or-results)
                 result))))])

      :pred
      (let [{:keys [::args ::pred-fn]} t]
        ['e nil (fn [_ _ result]
                  (let [args (map #(or (and (symbol? %) (result %)) %) args)]
                    (apply pred-fn args)))]))))

(defn term-symbols [terms]
  (reduce into #{}
          (for [[op t] terms]
            (condp = op
              :term (filter symbol? t)
              :not (filter symbol? t)
              :or (term-symbols (:terms t))
              :not-join (term-symbols (:terms t))
              :pred []))))

(defn- validate-query [{:keys [find where]}]
  (let [variables (term-symbols where)]
    (doseq [binding find]
      (when-not (variables binding)
        (throw (IllegalArgumentException. (str "Find clause references unbound variable: " binding)))))))

(defn- find-projection [find result]
  (map (partial get result) find))

(defn q
  ([db terms]
   (q db terms (java.util.Date.)))
  ([db {:keys [find where] :as q} ts]
   (let [{:keys [find where] :as q} (s/conform ::query q)]
     (validate-query q)
     (when (= :clojure.spec.alpha/invalid q)
       (throw (ex-info "Invalid input" (s/explain-data ::query q))))
     (->> where
          (query-terms->plan)
          (query-plan->results db ts)
          (map (partial find-projection find))
          (into #{})))))
