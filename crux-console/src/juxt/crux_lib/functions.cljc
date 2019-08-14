(ns juxt.crux-lib.functions
  #?(:cljs (:require [cljs.reader :as edn]))
  #?(:clj  (:require [clojure.tools.reader.edn :as edn])))

(defn- single-int-or-bool? [v]
  (let [v1 (first v)]
    (and (= 1 (count v))
         (or (nat-int? v1)
             (boolean? v1)))))

(defn- normalize-query-part [v]
  "this strips out limit/offset/full-results into singular variable"
  (if (single-int-or-bool? v)
    (first v)
    (vec v)))

; todo critical review target

(defn normalize-query [q]
  (cond
    (vector? q) (into {} (for [[[k] v] (->> (partition-by keyword? q)
                                            (partition-all 2))]
                           [k (normalize-query-part v)]))
    (string? q) (if-let [q (try
                             (edn/read-string q)
                             #?(:cljs (catch :default e e)
                                :clj (catch Exception e)))]
                  (normalize-query q)
                  q)
    :else
    q))
; adapted from https://github.com/juxt/crux/blob/3368595d7fcaec726b1a602a9ec75e325b49ecd6/src/crux/query.clj#L1013
