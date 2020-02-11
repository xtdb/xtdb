(ns crux-ui.routes
  (:require [bidi.bidi :as bidi]
            [re-frame.core :as rf]
            [crux-ui.logging :as log]
            [crux-ui.views.commons.dom :as dom]
            [crux-ui.functions :as f]
            [medley.core :as m]))

(def prefix
  (or (not-empty (dom/jsget js/document.documentElement "dataset" "routingPrefix"))
      "/console"))

(def ^:private routes
  [prefix
   {"" :rd/query-ui

    "/output"
    {"" :rd/query-ui
     ["/" :r/output-tab] :rd/query-ui-output-tab}

    "/settings"
    {"" :rd/settings
     ["/" :r/tab] :rd/settings}

    ["/example/" :rd/example-id] :rd/query-ui}])

(defn- prefix-keys [route]
  (f/map-keys #(keyword "r" (name %)) route))

(defn- match-route [path]
  (if-let [route (prefix-keys (bidi/match-route routes path))]
    (cond-> route
            (get-in route [:r/route-params :r/output-tab])
            (update-in [:r/route-params :r/output-tab]
                       #(keyword "db.ui.output-tab" %)))))


(def ^:private path-for (partial bidi/path-for routes))

(defn path-for-tab [tab-name {:r/keys [search]}]
  (str (path-for :rd/query-ui-output-tab
                 :r/output-tab
                 (name tab-name))
       search))

(defn query-str->map [^js/String query]
  (let [raw-map (into {} (js->clj (js/Array.from (js/URLSearchParams. query))))]
    (f/map-keys #(if (string? %) (keyword "rd" %) %) raw-map)))

(defn ^js/String query-map->str [m]
  (.toString (js/URLSearchParams. (clj->js m))))

(defn- url-for [route-vector query-map]
  (let [path (apply path-for route-vector)
        query-str (query-map->str query-map)]
    (str path "?" query-str)))


(defn- calc-route-data-from-location []
  (let [route-data (match-route js/location.pathname)
        query-map (query-str->map js/location.search)]
     (assoc route-data :r/query-params query-map)))

(defn- on-pop-state [evt]
  (rf/dispatch [:evt.sys/set-route (calc-route-data-from-location)]))

(defn- calc-query-url [route-data crux-query-str]
  (let [handler-id (route-data :r/handler)
        route-params (-> (:r/route-params route-data)
                         (m/update-existing :r/output-tab name))
        route-params (vec (flatten (into [handler-id] route-params)))]
    (println :route-params route-params)
    (url-for route-params {:rd/query crux-query-str})))

; (calc-query-url "wewew")

(defn- push-query [crux-query-str]
  (js/history.pushState nil "Crux Console : user query" (calc-query-url (calc-route-data-from-location) crux-query-str)))

(defn- on-link-clicks [click-evt]
  (let [target (dom/jsget click-evt "target")
        tagname (dom/jsget target "tagName")
        href (dom/jsget target "href")
        is-anchor? (= "A" tagname)
        url (if is-anchor?
              (try
                (js/URL. href)
                (catch js/Error e
                  (log/error "can't parse link url, will ignore it" href)
                  nil)))
        pathname (and url (dom/jsget url "pathname"))
        route-data (and pathname (match-route pathname))]
    (when (and url is-anchor? route-data)
      (.preventDefault click-evt)
      (js/history.pushState nil "Crux Console" href)
      (rf/dispatch [:evt.sys/set-route route-data]))))

(defn init []
  (js/window.addEventListener "popstate" on-pop-state false)
  (js/window.addEventListener "click" on-link-clicks true)
  (let [route-data (calc-route-data-from-location)]
    (rf/dispatch [:evt.sys/set-route route-data])))
