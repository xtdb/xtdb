(ns juxt.crux-ui.frontend.routes
  (:require [bidi.bidi :as bidi]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.logging :as log]
            [juxt.crux-ui.frontend.views.commons.dom :as dom]
            [juxt.crux-ui.frontend.functions :as f]))

(def routes
  ["/console"
   {"" :rd/query-ui

    "/settings"
       {"" :rd/settings
        ["/" :rd/tab] :rd/settings}

    ["/example/" :rd/example-id] :rd/query-ui}])

(defn- prefix-keys [route]
  (f/map-keys #(keyword "r" (name %)) route))

(def match-route (comp prefix-keys (partial bidi/match-route routes)))
(def path-for (partial bidi/path-for routes))

(comment
  (bidi/match-route routes "/console")
  (bidi/match-route routes "/console/settings")
  (bidi/match-route routes "/console/settings/network")
  (bidi/match-route routes "/console/example/network")
  (bidi/path-for routes :rd/settings))

(defn- on-pop-state [evt]
  (log/log :on-pop-state evt)
  (let [route-data (match-route js/location.pathname)]
    (rf/dispatch [:evt.sys/set-route route-data])))

(defn- on-link-clicks [click-evt]
  (let [target (dom/jsget click-evt "target")
        tagname (dom/jsget target "tagName")
        href (dom/jsget target "href")
        url (js/URL. href)
        pathname (dom/jsget url "pathname")
        rd (and pathname (match-route pathname))]
    (log/log :click-evt click-evt tagname href)
    (when (and (= "A" tagname) rd)
      (.preventDefault click-evt)
      (js/history.pushState nil "Console" href)
      (rf/dispatch [:evt.sys/set-route rd]))))

(defn init []
  (js/window.addEventListener "popstate" on-pop-state false)
  (js/window.addEventListener "click" on-link-clicks true)
  (let [route-data (match-route js/location.pathname)]
    (rf/dispatch [:evt.sys/set-route route-data])))
