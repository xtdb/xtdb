(ns xtdb.ui.common
  (:require
   [goog.string :as gstring]
   [goog.string.format]
   [cljs.reader :as reader]
   [clojure.pprint :as pprint]
   [clojure.string :as string]
   [xtdb.ui.navigation :as navigation]
   [reitit.frontend :as reitit]
   [reitit.frontend.easy :as rfe]
   [tick.alpha.api :as t]
   [tick.format :as tf]
   [clojure.walk :as walk]
   [tick.locale-en-us]))

(defn route->url
  "k: page handler i.e. :entity
  params: path-params map
  query: query-params map i.e. {:find '[?eid]'}"
  ([k]
   (route->url k nil nil))
  ([k params]
   (route->url k params nil))
  ([k params query]
   (rfe/href k params query)))

(defn url->route
  "url: abosolute string path i.e. '/_xtdb/entity?eid-edn=...'"
  [url]
  (reitit/match-by-path (navigation/router) url))

(defn iso-format-datetime
  [dt]
  (try
    (when dt
      (t/format (tf/formatter "yyyy-MM-dd'T'HH:mm:ss.SSSXXX") (t/zoned-date-time (t/inst dt))))
    (catch js/Error e
      nil)))

(defn edn->pretty-string
  [obj]
  (with-out-str (pprint/pprint obj)))

(defn format-duration->seconds
  [duration]
  (when duration
    (gstring/format "%d.%03d" (t/seconds duration) (- (t/millis duration) (* 1000 (t/seconds duration))))))

(defn vectorize
  [ks m]
  (map (fn [[k v]]
         (if (and (some #(= k %) ks)
                  (string? v))
           [k (vector v)]
           [k v])) m))

(defn query->formatted-query-string [query]
  (with-out-str
    (pprint/with-pprint-dispatch
      pprint/code-dispatch
      (pprint/pprint query))))

(defn edn->query-params
  [edn]
  (->> edn
       (map
        (fn [[k v]]
          [k (if (or (= :where k) (= :args k) (= :order-by k) (= :rules k))
               (mapv str v)
               (str v))]))
       (into {})))

(defn- scroll-top []
  (set! (.. js/document -body -scrollTop) 0)
  (set! (.. js/document -documentElement -scrollTop) 0))

(defn arrow-svg
  [toggled?]
  [:svg
   {:class (if toggled? "arrow-toggle" "arrow-untoggle")
    :height "24"
    :id "arrow"
    :viewBox "0 0 24 24"
    :width "24"}
   [:g {:fill "#111111"}
    [:path {:d "M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"}]]])

(defn sort-map [map]
  (->> map
       (walk/postwalk
        (fn [map] (cond->> map
                    (map? map) (into (sorted-map)))))))
