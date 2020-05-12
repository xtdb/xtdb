(ns crux.ui.common
  (:require
   [cljs.reader :as reader]
   [clojure.pprint :as pprint]
   [clojure.string :as string]
   [crux.ui.navigation :as navigation]
   [reitit.frontend :as reitit]
   [reitit.frontend.easy :as rfe]
   [tick.alpha.api :as t]))

(defn route->url
  "k: page handler i.e. :entity
  params: path-params map i.e. {:eid ':hello'}
  query: query-params map i.e. {:find '[?eid]'}"
  ([k]
   (route->url k nil nil))
  ([k params]
   (route->url k params nil))
  ([k params query]
   (rfe/href k params query)))

(defn url->route
  "url: abosolute string path i.e. '/_entity/:eid?tt=...'"
  [url]
  (reitit/match-by-path (navigation/router) url))

(defn date-time->datetime
  "d: 2020-04-28
  t: 15:45:45.935"
  [d t]
  (when (and (not-empty d) (not-empty t))
    (str (t/date d) "T" (t/time t))))

(defn datetime->date-time
  "dt: 2020-04-28T15:45:45.935"
  [dt]
  (when (not-empty dt)
    {:date (str (t/date dt))
     :time (str (t/time dt))}))

(defn query-params->formatted-edn-string
  [query-params-map]
  (when-let [formatted
             (not-empty
              (try
                (reader/read-string
                 (string/replace (str query-params-map)  "\"" ""))
                (catch :default _ {})))]
    (with-out-str
      (pprint/with-pprint-dispatch
        pprint/code-dispatch
        (pprint/pprint formatted)))))

(defn edn->query-params
  [edn]
  (->> edn
       (map
        (fn [[k v]]
          [k (if (vector? (first v))
               (mapv str v)
               (str v))]))
       (into {})))

(defn back-page
  []
  (js/window.history.back))

(defn- scroll-top []
  (set! (.. js/document -body -scrollTop) 0)
  (set! (.. js/document -documentElement -scrollTop) 0))
