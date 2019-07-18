(ns juxt.crux-ui.frontend.logic.query-analysis
  (:require [medley.core :as m]
            [cljs.reader]))


(defn calc-vector-headers [query-vector]
  (->> query-vector
       (drop-while #(not= :find %))
       (rest)
       (take-while #(not= (keyword? %)))))


(defn analyze-full-results-headers [query-results-seq]
  (let [res-count (count query-results-seq)
        sample (if (> res-count 50)
                 (random-sample (/ 50 res-count) query-results-seq)
                 query-results-seq)]
    (set (flatten (map (comp keys :crux.query/doc) sample)))))

(defn query-vec->map [qv]
  (let [raw-map
          (->> qv
               (partition-by keyword?)
               (partition 2)
               (into {}))]
    (-> raw-map
        (update :find vec)
        (m/update-existing :full-results? first)
        (update :where vec))))

(defn single-tx-vec->map [[type doc-id doc vt tt]]
  {:crux.ui/query-type :crux.ui.query-type/tx-single
   :crux.tx/type type
   :crux.db/id   doc-id
   :crux.db/doc  doc
   :crux.ui/vt   vt
   :crux.ui/tt   tt})

(defn multi-tx-vec->map [txes-vector]
  (let [tx-infos (map single-tx-vec->map txes-vector)]
    {:crux.ui/query-type :crux.ui.query-type/tx-multi
     :tx-count (count tx-infos)
     :tx-infos tx-infos}))

(defn try-read-string [input-str]
  (try
    (cljs.reader/read-string input-str)
    (catch js/Error e
      {:error e})))

(defn query-vector? [edn]
  (and (vector? edn) (= :find (first edn))))

(def crux-tx-types-set
  #{:crux.tx/put :crux.tx/cas :crux.tx/delete :crux.tx/evict})

(defn single-tx-vector? [edn]
  (and (vector? edn) (crux-tx-types-set (first edn))))

(defn multi-tx-vector? [edn]
  (and (vector? edn) (not-empty edn) (every? single-tx-vector? edn)))

(defn query-map? [edn]
  (and (map? edn) (every? edn [:find :where])))

(defn third [coll]
  (nth coll 2 nil))

(defn infer-symbol-attr-map
  "Given a simple datalog query map returns a map symbol -> attribute
  "
  [qmap]
  (let [symbols-queried   (set (:find qmap))
        where-vec         (:where qmap)
        attr-triplets     (filter (comp keyword? second) where-vec)
        eid-triplets      (filter (comp symbol? first) where-vec)
        retained-triplets (filter #(symbols-queried (third %)) attr-triplets)
        eid-symbols       (set (map first eid-triplets))
        retained-eids     (set (filter symbols-queried eid-symbols))
        lookup-symbol-attr (fn [symbol]
                             (second (m/find-first #(= (third %) symbol) retained-triplets)))
        symbol-attr-pair  (fn [symbol]
                            [symbol
                              (if (retained-eids symbol)
                                :crux.db/id
                                (lookup-symbol-attr symbol))])
        symbol->attr      (into {} (map symbol-attr-pair symbols-queried))]
    symbol->attr))


(defn with-query-map-data [qmap]
  (assoc qmap :crux.ui/query-type :crux.ui.query-type/query
              :query/attributes (infer-symbol-attr-map qmap)))

(defn analyse-query [input-edn]
  (cond
    (query-vector? input-edn)     (with-query-map-data (query-vec->map input-edn))
    (single-tx-vector? input-edn) (single-tx-vec->map input-edn)
    (multi-tx-vector?  input-edn) (multi-tx-vec->map input-edn)
    (query-map? input-edn)        (with-query-map-data input-edn)
    :else                         false))

(defn analyse-results [query-info results]
  (if (not-empty results)
    (let [r-count (count results)
          ids-queried? false
          full-results? (:full-results? query-info)
          first-res (if full-results?
                      (-> results first :crux.query/doc)
                      (-> first results))
          ids-pluck
          (cond
            full-results? :crux.db/id
            ids-queried? first
            :else identity)
          attrs (if full-results?
                  (keys first-res)
                  (:query/attributes query-info))
          ids (if ids-queried? (map ids-pluck results))]
      {:single-entity? (= 1 r-count)
       :results-count  r-count
       :entity-ids     ids
       :entity-id      (first ids)
       :first-res      first-res
       :attributes     attrs})))
