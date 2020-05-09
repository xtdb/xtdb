(ns crux.ui.common
  (:require
   [crux.ui.navigation :as navigation]
   [reitit.frontend :as reitit]
   [reitit.frontend.easy :as rfe]))

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

(defn back-page
  []
  js/window.history.back)

(defn- scroll-top []
  (set! (.. js/document -body -scrollTop) 0)
  (set! (.. js/document -documentElement -scrollTop) 0))
