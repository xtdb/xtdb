(ns juxt.crux-ui.frontend.functions
  (:require [cljs.core.async :as async]
            [cljs.pprint :as pprint])
  (:import [goog.object]
           [goog.async Debouncer]))

(def jsget goog.object/getValueByKeys)

(defn pprint-str [edn]
  (with-out-str (pprint/pprint edn)))

(defn merge-with-keep [a]
  (apply merge-with (fn [v1 v2] ((if (vector? v1) conj vector) v1 v2)) a))

(defn debounce [f interval]
  (let [dbnc (Debouncer. f interval)]
    ;; We use apply here to support functions of various arities
    (fn [& args] (.apply (.-fire dbnc) dbnc (to-array args)))))

(defn index-by
  "indexes a finite collection"
  [key-fn coll]
  (loop [remaining-items (seq coll)
         index-map (transient {})]
    (if-let [x (first remaining-items)]
      (recur (next remaining-items)
             (assoc! index-map (key-fn x) x))
      (persistent! index-map))))


(defn map-map-values-vec [f m]
  (into {} (for [[k vs] m] [k (mapv f vs)])))

(defn map-keys [f m]
  (into {} (for [[k v] m] [(f k) v])))

(defn map-values [f m]
  (into {} (for [[k v] m] [k (f v)])))

(defn ks-vs-to-map [ks vs]
  (merge-with-keep (map #(zipmap ks %) vs)))

(defn positions
  [pred coll]
  (keep-indexed (fn [idx x]
                  (when (pred x)
                    idx))
                coll))

(defn qsort [f sb]
  (apply juxt (map (fn [s] (fn [x] (nth x (first (positions #{s} f))))) sb)))


(defn format-date [d]
  (.toISOString (new js/Date d)))

(defn decode-html [html]
  (let [txt (.createElement js/document "textarea")]
    (set! (.-innerHTML txt) html)
    (.. txt -value)))

(defn strreplace [s]
  (clojure.string/replace s #"(?:\\r\\n|\\r|\\n)" "<br />"))
;/(?:\r\n|\r|\n)/g

(defn e0? [v]
  (= 'e (first v)))

