(ns juxt.crux-ui.frontend.views.facade
  (:require [garden.core :as garden]
            [juxt.crux-ui.frontend.views.query-ui :as q]
            [juxt.crux-ui.frontend.views.header :as header]
            [juxt.crux-ui.frontend.views.comps :as comps]))


(def root-styles
  (garden/css
    [[:a
      {:color "hsl(32, 91%, 54%)"}
      [:&:visited {:color "hsl(32, 91%, 54%)"}]]
     [:button
       {:font-size :1rem}]
     comps/button-textual-styles
     [:html :body :#app
      {:font-family "Helvetica Neue, Helvetica, BlinkMacSystemFont, -apple-system, Roboto, 'Segoe UI', sans-serif"
       :height "100%"
       :font-weight 300}]

     [:.root
      {:display :grid
       :place-items :stretch
       :height "100%"
       :grid-template "'header' 100px
                      'body' calc(100% - 100px)"}
      [:&__header
       {:grid-area :header}]
      [:&__body
       {:grid-area :body
        :display :flex
        :align-items :center
        :justify-content :center}]]]))


(defn root []
  [:div#root.root
   [:style root-styles]
   [:div.root__header
     [header/root]]
   [:div.root__body
     [q/query-ui]]])
