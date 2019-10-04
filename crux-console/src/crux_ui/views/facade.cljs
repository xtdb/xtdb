(ns crux-ui.views.facade
  (:require [garden.core :as garden]
            [garden.stylesheet :as gs]
            [re-frame.core :as rf]
            [crux-ui.views.query-ui :as q]
            [crux-ui.views.header :as header]
            [crux-ui.views.commons.tiny-components  :as comps]
            [crux-ui.views.settings :as settings]
            [crux-ui.svg-icons :as icon]
            [crux-ui.views.second-layer :as second-layer]
            [crux-ui.views.commons.input :as input]
            [crux-ui.functions :as f]))


(def color-link "hsl(32, 61%, 64%)")
(def color-link--hover "hsl(32, 91%, 54%)")

(defn on-sidebar-bg-click [evt]
  (if (= "root__sidebar" (some-> evt (f/jsget "target" "id")))
    (rf/dispatch [:evt.ui.second-layer/toggle])))

(def ^:private root-styles
  [:style
    (garden/css
      [[:a
        {:color color-link}
        [:&:visited {:color color-link}]
        [:&:hover {:color color-link--hover}]]
       [:h1 :h2
        {:font-weight 300
         :letter-spacing :0.19em}]
       [:h3 :h4
        {:font-weight 300
         :letter-spacing :0.05em}]
       [:.g-nolink
        :.g-nolink:active
        :.g-nolink:visited
        {:text-decoration :inherit
         :color :inherit}]
       [:button
         {:font-size :1rem}]
       input/styles
       icon/styles
       comps/button-styles
       [:html :body :#app
        {:font-family "Helvetica Neue, Helvetica, BlinkMacSystemFont, -apple-system, Roboto, 'Segoe UI', sans-serif"
         :height "100%"
         :font-weight 400}]

       [:.root
        {:display :grid
         :place-items :stretch
         :height :100%
         :grid-template
         "'header' 84px
          'body' calc(100% - 84px)"}
        [:&--page
          {:grid-template
           "'header' 84px
            'body' 1fr"}]
        [:&__header
         {:grid-area :header}]
        [:&__second-layer
         {:position :fixed
          :top :0px
          :bottom :0px
          :border-radius :2px
          :left :0px
          :right 0
          :z-index 100
          :background "hsla(0, 0%, 0%, .3)"}]
        [:&__body
         {:grid-area :body
          :display :flex
          :overflow :hidden
          :align-items :center
          :justify-content :center}]]

       (gs/at-media {:max-width :375px}
         [:h1
          {:font-size :30px
           :line-height 2.0
           :letter-spacing :0.08em
           :font-weight 400}]
         [:h2
          {:font-size :24px}])

       (gs/at-media {:max-width :375px}
         [:.root
          {:grid-template
           "'header' 64px
            'body' calc(100% - 64px)"}])])])


(defn root []
  (let [-sub-root-tab (rf/subscribe [:subs.ui/root-tab])
        -sub-second-layer (rf/subscribe [:subs.db.ui/second-layer])]
    (fn []
      [:div#root.root
       root-styles
       [:div.root__header
        [header/root]]
       (if @-sub-second-layer
         [:div#root__second-layer.root__second-layer {:on-click on-sidebar-bg-click}
          [second-layer/root]])
       [:div.root__body
        (case @-sub-root-tab
          :db.ui.root-tab/query-ui [q/query-ui]
          :db.ui.root-tab/settings [settings/root]
          [q/query-ui])]])))
