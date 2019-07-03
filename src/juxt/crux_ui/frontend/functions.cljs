(ns juxt.crux-ui.frontend.functions
  (:require [cljs.core.async :as async]
            [cljs.pprint :as pprint]))

(def jsget goog.object/getValueByKeys)

(defn pprint-str [edn]
  (with-out-str (pprint/pprint edn)))

(defn fetch [path c & [opts]]
  (.then (.then (js/fetch (str "http://localhost:8080/" path) opts) #(.text %)) #(async/put! c %)))

(defn fetch2 [path c & [opts]]
  (.then (.then (js/fetch (str "http://localhost:8080/" path) opts) #(.text %)) #(do (async/put! c %) (async/close! c))))

(defn merge-with-keep [a]
  (apply merge-with (fn [v1 v2] ((if (vector? v1) conj vector) v1 v2)) a))

(defn map-map-values-vec [f m]
  (into {} (for [[k vs] m] [k (mapv f vs)])))

(defn map-map-values [f m]
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

