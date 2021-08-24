(ns ^:no-doc crux.pull
  (:require [crux.codec :as c]
            [crux.db :as db]
            [crux.io :as cio]
            [juxt.clojars-mirrors.eql.v2021v02v28.edn-query-language.core :as eql]
            [clojure.string :as string])
  (:import clojure.lang.MapEntry))

(defn- recognise-union [child]
  (when (and (= :join (:type child))
             (= :union (get-in child [:children 0 :type])))
    :union))

(defn- replace-docs [v docs]
  (if (not-empty (::hashes (meta v)))
    (v docs)
    v))

(defn- lookup-docs [v {:keys [document-store]}]
  (when-let [hashes (not-empty (::hashes (meta v)))]
    (->> (db/fetch-docs document-store hashes)
         (cio/map-vals c/crux->xt))))

(defmacro let-docs {:style/indent 1} [[binding hashes] & body]
  `(-> (fn ~'let-docs [~binding]
         ~@body)
       (with-meta {::hashes ~hashes})))

(defn- after-doc-lookup [f lookup]
  (if-let [hashes (::hashes (meta lookup))]
    (let-docs [docs hashes]
      (let [res (replace-docs lookup docs)]
        (if (::hashes (meta res))
          (after-doc-lookup f res)
          (f res))))
    (f lookup)))

(defn- raise-doc-lookup-out-of-coll
  "turns a vector/set where each of the values could be doc lookups into a single doc lookup returning a vector/set"
  [coll]
  (if-let [hashes (not-empty (into #{} (mapcat (comp ::hashes meta)) coll))]
    (let-docs [docs hashes]
      (->> coll
           (into (empty coll) (map #(replace-docs % docs)))
           raise-doc-lookup-out-of-coll))
    coll))

(defrecord RecurseState [seen-vs child-fns])

(declare pull-child-fns)

(defn- ->pull-child [{:keys [query] :as join}]
  (letfn [(pull-child [v db ^RecurseState recurse-state]
            (if (contains? (.seen-vs recurse-state) v)
              {:xt/id v}
              (let [recurse-state ^RecurseState (update recurse-state :seen-vs conj v)]
                (->> (mapv (fn [f]
                             (f v db recurse-state))
                           (.child-fns recurse-state))
                     (raise-doc-lookup-out-of-coll)
                     (after-doc-lookup (fn [res]
                                         (into {} (mapcat identity) res)))))))]
    (cond
      (= '... query) pull-child
      (int? query) (fn [v db ^RecurseState recurse-state]
                     (when (<= (count (.seen-vs recurse-state)) ^long query)
                       (pull-child v db recurse-state)))
      :else (let [recurse-state (RecurseState. #{} (pull-child-fns join))]
              (fn [v db _]
                (pull-child v db recurse-state))))))

(defn- forward-joins-child-fn [{:keys [props special forward-joins unions]}]
  (when-not (every? empty? [props special forward-joins unions])
    (let [forward-join-child-fns (for [{:keys [dispatch-key] :as join} forward-joins]
                                   (let [into-coll (get-in join [:params :into])
                                         limit (get-in join [:params :limit])
                                         pull-child (->pull-child join)
                                         k (or (get-in join [:params :as] dispatch-key))]
                                     (fn [doc db recurse-state]
                                       (when-let [v (get doc dispatch-key)]
                                         (->> (if (c/multiple-values? v)
                                                (->> (cond->> v
                                                       limit (take limit))
                                                     (mapv #(pull-child % db recurse-state))
                                                     (raise-doc-lookup-out-of-coll))
                                                (pull-child v db recurse-state))
                                              (after-doc-lookup (fn [res]
                                                                  (when res
                                                                    (MapEntry/create k (cond->> res
                                                                                         into-coll (into into-coll)))))))))))

          union-child-fns (for [{:keys [dispatch-key children]} unions
                                {:keys [union-key] :as child} (get-in children [0 :children])]
                            (let [pull-child (->pull-child child)]
                              (fn [value doc db recurse-state]
                                (->> (c/vectorize-value (get doc dispatch-key))
                                     (keep (fn [v]
                                             (when (= v union-key)
                                               (pull-child value db recurse-state))))))))

          prop-child-fns (->> (for [{:keys [dispatch-key params]} props]
                                (let [{into-coll :into, :keys [as limit]} params
                                      k (or as dispatch-key)]
                                  (MapEntry/create dispatch-key
                                                   (fn [_ v]
                                                     (MapEntry/create k (-> v
                                                                            (cond->> limit (take limit)
                                                                                     into-coll (into into-coll))))))))
                              (into {}))

          default-prop-fn (if (contains? (into #{} (map :dispatch-key) special) '*)
                            (fn [k v] (MapEntry/create k v))
                            (fn [_k _v] nil))

          prop-defaults (->> (for [{:keys [dispatch-key], {:keys [default as]} :params} props
                                   :when default]
                               (MapEntry/create (or as dispatch-key) default))
                             (into {}))]

      (fn [value {:keys [entity-resolver-fn] :as db} recurse-state]
        (when-let [content-hash (some-> (entity-resolver-fn (c/->id-buffer value))
                                        c/new-id)]
          (let-docs [docs #{content-hash}]
            (let [doc (get docs content-hash)]
              (->> (concat (->> forward-join-child-fns (map (fn [f] (f doc db recurse-state))))
                           (->> union-child-fns (mapcat (fn [f] (f value doc db recurse-state)))))
                   (raise-doc-lookup-out-of-coll)
                   (after-doc-lookup (fn [res]
                                       ;; TODO do we need a deeper merge here?
                                       (into (into prop-defaults
                                                   (keep (fn [[k v]]
                                                           ((get prop-child-fns k default-prop-fn) k v)))
                                                   doc)
                                             res)))))))))))

(defn- reverse-joins-child-fn [reverse-joins]
  (when-not (empty? reverse-joins)
    (let [reverse-join-child-fns (for [{:keys [dispatch-key] :as join} reverse-joins]
                                   (let [into-coll (get-in join [:params :into])
                                         limit (get-in join [:params :limit])
                                         forward-key (keyword (namespace dispatch-key)
                                                              (subs (name dispatch-key) 1))
                                         one? (= :one (get-in join [:params :cardinality]))
                                         pull-child (->pull-child join)
                                         k (or (get-in join [:params :as]) dispatch-key)]
                                     (fn [value-buffer {:keys [index-snapshot entity-resolver-fn] :as db} recurse-state]
                                       (->> (vec (for [v (cond->> (db/ave index-snapshot (c/->id-buffer forward-key) value-buffer nil entity-resolver-fn)
                                                           one? (take 1)
                                                           limit (take limit)
                                                           :always vec)]
                                                   (pull-child (db/decode-value index-snapshot v) db recurse-state)))
                                            (raise-doc-lookup-out-of-coll)
                                            (after-doc-lookup (fn [res]
                                                                (when-let [res (seq (remove nil? res))]
                                                                  (MapEntry/create k (cond->> res
                                                                                       into-coll (into into-coll)
                                                                                       one? first)))))))))]
      (fn [value db recurse-state]
        (let [value-buffer (c/->value-buffer value)]
          (->> reverse-join-child-fns
               (keep (fn [f]
                       (f value-buffer db recurse-state)))
               (raise-doc-lookup-out-of-coll)))))))

(defn- pull-child-fns [pull-spec]
  (let [{special :special,
         props :prop,
         joins :join,
         unions :union} (->> (:children pull-spec)
                             (group-by (some-fn recognise-union
                                                :type
                                                (constantly :special))))

        {forward-joins false, reverse-joins true}
        (group-by (comp #(string/starts-with? % "_") name :dispatch-key) joins)]

    (->> [(forward-joins-child-fn {:special special, :props props, :forward-joins forward-joins, :unions unions})
          (reverse-joins-child-fn reverse-joins)]
         (remove nil?))))

(defn compile-pull-spec [pull-spec]
  (let [pull-child (->pull-child (eql/query->ast pull-spec))]
    (fn [value db]
      (pull-child value db nil))))

(defn ->pull-result [db compiled-find q-conformed res]
  (->> res
       (map (fn [row]
              (mapv (fn [value ->result]
                      (->result value db))
                    row
                    (mapv :->result compiled-find))))
       (partition-all (or (:batch-size q-conformed)
                          (:batch-size db)
                          100))
       (map (fn [results]
              (->> results
                   (mapv raise-doc-lookup-out-of-coll)
                   raise-doc-lookup-out-of-coll)))
       (mapcat (fn [lookup]
                 (if (::hashes (meta lookup))
                   (recur (replace-docs lookup (lookup-docs lookup db)))
                   lookup)))))
