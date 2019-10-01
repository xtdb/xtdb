(ns crux-ui.views.facade-perf
  (:require [garden.core :as garden]
            [crux-ui.views.query-perf :as query-perf]
            [crux-ui.views.header :as header]
            [crux-ui.views.commons.tiny-components  :as comps]
            [crux-ui.svg-icons :as icon]))

(def ^:private root-styles
  [:style
    (garden/css
      [[:a
        {:color "hsl(32, 91%, 54%)"}
        [:&:visited {:color "hsl(32, 91%, 54%)"}]]
       [:button
         {:font-size :1rem}]
       icon/styles
       comps/button-styles
       [:html :body :#app
        {:font-family "Helvetica Neue, Helvetica, BlinkMacSystemFont, -apple-system, Roboto, 'Segoe UI', sans-serif"
         :height "100%"
         :font-weight 300}]

       [:.root
        {:display :grid
         :place-items :stretch
         :height "100%"
         :grid-template
         "'header' 100px
          'body' calc(100% - 100px)"}
        [:&--page
          {:grid-template
           "'header' 100px
            'body' 1fr"}]
        [:&__header
         {:grid-area :header}]
        [:&__body
         {:grid-area :body
          :display :flex
          :align-items :center
          :justify-content :center}]]])])


(defn root []
  [:div#root.root.root--page
   root-styles
   [:div.root__header
     [header/root]]
   [:div.root__body.root__body--page
     [query-perf/root]]])

