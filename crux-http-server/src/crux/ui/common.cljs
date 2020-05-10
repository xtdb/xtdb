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

(defn instant->date-time
  [t]
  (when t (str (t/date-time (t/instant t)))))

(defn date-time->instant
  [t]
  (when t (str (t/instant (t/date-time t)))))

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

(defn back-page
  []
  (js/window.history.back))

(defn- scroll-top []
  (set! (.. js/document -body -scrollTop) 0)
  (set! (.. js/document -documentElement -scrollTop) 0))
