(ns ^:no-doc crux.eql-project
  (:require [crux.codec :as c]
            [crux.db :as db]
            [edn-query-language.core :as eql]
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
    (db/fetch-docs document-store hashes)))

(defmacro let-docs [[binding hashes] & body]
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

(defn- project-child [v db child-fns]
  (->> (mapv (fn [f]
               (f v db))
             child-fns)
       (raise-doc-lookup-out-of-coll)
       (after-doc-lookup (fn [res]
                           (into {} (mapcat identity) res)))))

(declare project-child-fns)

(defn- forward-joins-child-fn [{:keys [props special forward-joins unions]}]
  (when-not (every? empty? [props special forward-joins unions])
    (let [forward-join-child-fns (for [{:keys [dispatch-key] :as join} forward-joins]
                                   (let [child-fns (project-child-fns join)]
                                     (fn [doc db]
                                       (let [v (get doc dispatch-key)]
                                         (->> (if (c/multiple-values? v)
                                                (->> (into [] (map #(project-child % db child-fns)) v)
                                                     (raise-doc-lookup-out-of-coll))
                                                (project-child v db child-fns))
                                              (after-doc-lookup (fn [res]
                                                                  (MapEntry/create dispatch-key res))))))))
          union-child-fns (for [{:keys [dispatch-key children]} unions
                                {:keys [union-key] :as child} (get-in children [0 :children])]
                            (let [child-fns (project-child-fns child)]
                              (fn [value doc db]
                                (->> (c/vectorize-value (get doc dispatch-key))
                                     (keep (fn [v]
                                             (when (= v union-key)
                                               (project-child value db child-fns))))))))
          prop-dispatch-keys (into #{} (map :dispatch-key) props)
          project-star? (contains? (into #{} (map :dispatch-key) special) '*)]

      (fn [value {:keys [entity-resolver-fn] :as db}]
        (let [content-hash (entity-resolver-fn (c/->id-buffer value))]
          (let-docs [docs #{content-hash}]
            (let [doc (get docs (c/new-id content-hash))]
              (->> (concat (->> forward-join-child-fns (map (fn [f] (f doc db))))
                           (->> union-child-fns (mapcat (fn [f] (f value doc db)))))
                   (raise-doc-lookup-out-of-coll)
                   (after-doc-lookup (fn [res]
                                       ;; TODO do we need a deeper merge here?
                                       (into (if project-star?
                                               doc
                                               (select-keys doc prop-dispatch-keys))
                                             res)))))))))))

(defn- reverse-joins-child-fn [reverse-joins]
  (when-not (empty? reverse-joins)
    (let [reverse-join-child-fns (for [{:keys [dispatch-key] :as join} reverse-joins]
                                   (let [child-fns (project-child-fns join)
                                         forward-key (keyword (namespace dispatch-key)
                                                              (subs (name dispatch-key) 1))
                                         one? (= :one (get-in join [:params :crux/cardinality]))]
                                     (fn [value-buffer {:keys [index-snapshot entity-resolver-fn] :as db}]
                                       (->> (vec (for [v (cond->> (db/ave index-snapshot (c/->id-buffer forward-key) value-buffer nil entity-resolver-fn)
                                                           one? (take 1)
                                                           :always vec)]
                                                   (project-child (db/decode-value index-snapshot v) db child-fns)))
                                            (raise-doc-lookup-out-of-coll)
                                            (after-doc-lookup (fn [res]
                                                                (MapEntry/create dispatch-key
                                                                                 (cond->> res
                                                                                   one? first))))))))]
      (fn [value db]
        (let [value-buffer (c/->value-buffer value)]
          (->> (for [f reverse-join-child-fns]
                 (f value-buffer db))
               (raise-doc-lookup-out-of-coll)))))))

(defn- project-child-fns [project-spec]
  (let [{special :special,
         props :prop,
         joins :join,
         unions :union} (->> (:children project-spec)
                             (group-by (some-fn recognise-union
                                                :type
                                                (constantly :special))))

        {forward-joins false, reverse-joins true}
        (group-by (comp #(string/starts-with? % "_") name :dispatch-key) joins)]

    (->> [(forward-joins-child-fn {:special special, :props props, :forward-joins forward-joins, :unions unions})
          (reverse-joins-child-fn reverse-joins)]
         (remove nil?))))

(defn compile-project-spec [project-spec]
  (let [root-fns (project-child-fns (eql/query->ast project-spec))]
    (fn [value db]
      (project-child value db root-fns))))

(defn ->project-result [db compiled-find q-conformed res]
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
