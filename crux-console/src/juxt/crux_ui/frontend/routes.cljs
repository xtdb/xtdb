(ns juxt.crux-ui.frontend.routes
  (:require [bidi.bidi :as bidi]
            [re-frame.core :as rf]))

(def routes
  ["/console"
   {"" :rd/query-ui

    "/settings"
       {"" :rd/settings
        ["/" :rd/tab] :rd/settings}

    ["/example/" :rd/example-id] :rd/query-ui}])

(def match (partial bidi/match-route routes))
(def path-for (partial bidi/path-for routes))

(comment
  (bidi/match-route routes "/console")
  (bidi/match-route routes "/console/settings")
  (bidi/match-route routes "/console/settings/network")
  (bidi/match-route routes "/console/example/network")
  (bidi/path-for routes :rd/settings))

(defn init []
  (let [route-data (match js/location.pathname)]
    (rf/dispatch [:evt.sys/set-route route-data])))
