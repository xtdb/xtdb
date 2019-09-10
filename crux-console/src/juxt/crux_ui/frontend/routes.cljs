(ns juxt.crux-ui.frontend.routes
  (:require [bidi.bidi :as bidi]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.logging :as log]
            [juxt.crux-ui.frontend.views.commons.dom :as dom]
            [juxt.crux-ui.frontend.functions :as f]))

(def ^:private routes
  ["/console"
   {"" :rd/query-ui

    "/settings"
       {"" :rd/settings
        ["/" :rd/tab] :rd/settings}

    ["/example/" :rd/example-id] :rd/query-ui}])

(defn- prefix-keys [route]
  (f/map-keys #(keyword "r" (name %)) route))

(def ^:private match-route (comp prefix-keys (partial bidi/match-route routes)))
(def ^:private path-for (partial bidi/path-for routes))

(defn query-str->map [^js/String query]
  (let [raw-map (into {} (js->clj (js/Array.from (js/URLSearchParams. query))))]
    (f/map-keys #(if (string? %) (keyword "rd" %) %) raw-map)))

(defn ^js/String query-map->str [m]
  (.toString (js/URLSearchParams. (clj->js m))))

(defn- url-for [route-id query-map]
  (let [path (path-for route-id)
        query-str (query-map->str query-map)]
    (str path "?" query-str)))



(comment
  (bidi/match-route routes "/console")
  (bidi/match-route routes "/console/settings")
  (bidi/match-route routes "/console/settings/network")
  (bidi/match-route routes "/console/example/network")
  (bidi/path-for routes :rd/settings)
  (query-str->map "wewe=wee&333=33&eeqq=33&a[]=3&a[]=6")
  (query-str->map js/location.search)
  (query-str->map "a=b%2Cc%2Cb")
  (query-str->map "a=%5B1%2C2%2C3%5D")
  (url-for :rd/query-ui {:rd/query (pr-str '{:find [e p] :where []})})
  (query-map->str
    {:rd/query
     (js/JSON.stringify (clj->js [1 2 3]))}))


(defn- calc-route-data-from-location []
  (let [route-data (match-route js/location.pathname)
        query-map (query-str->map js/location.search)]
     (assoc route-data :r/query-params query-map)))

(defn- on-pop-state [evt]
  (rf/dispatch [:evt.sys/set-route (calc-route-data-from-location)]))

(defn- push-query [crux-query-str]
  (let [url (url-for :rd/query-ui {:rd/query crux-query-str})]
    (js/history.pushState nil "Crux Console : user query" url)))

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
