(ns juxt.crux-ui.frontend.views.facade
  (:require [garden.core :as garden]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.query-ui :as q]
            [juxt.crux-ui.frontend.views.header :as header]
            [juxt.crux-ui.frontend.views.comps :as comps]
            [juxt.crux-ui.frontend.views.settings :as settings]
            [juxt.crux-ui.frontend.svg-icons :as icon]))

(def ^:private -sub-root-tab (rf/subscribe [:subs.ui/root-tab]))

(def ^:private root-styles
  [:style
    (garden/css
      [[:a
        {:color "hsl(32, 91%, 54%)"}
        [:&:visited {:color "hsl(32, 91%, 54%)"}]]
       [:button
         {:font-size :1rem}]
       icon/styles
       comps/button-textual-styles
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
  (let [root-tab @-sub-root-tab]
    [:div#root.root {:class (if (= :db.ui.root-tab/query-perf root-tab) "root--page")}
     root-styles
     [:div.root__header
       [header/root]]
     [:div.root__body {:class (if (= :db.ui.root-tab/query-perf root-tab) "root__body--page")}
       (case root-tab
         :db.ui.root-tab/query-ui [q/query-ui]
         :db.ui.root-tab/settings [settings/root]
         [q/query-ui])]]))
