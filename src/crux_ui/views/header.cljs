(ns crux-ui.views.header
  (:require [garden.core :as garden]
            [garden.stylesheet :as gs]
            [re-frame.core :as rf]
            [crux-ui.routes :as routes]
            [crux-ui.views.commons.tabs :as tabs]
            [crux-ui.views.commons.css-logo :as css-logo]
            [crux-ui.views.node-status :as node-status]
            [crux-ui.views.commons.input :as input]
            [crux-ui.views.commons.tiny-components :as comps]
            [crux-ui.config :as cfg]))


(def ^:private -sub-display-mode (rf/subscribe [:subs.ui/display-mode]))

(defn disable-toggle-display-mode []
  (rf/dispatch [:evt.ui.display-mode/toggle]))

(defn dispatch-second-layer-toggle []
  (rf/dispatch [:evt.ui.second-layer/toggle]))


(def ^:private header-styles
  [:style
   (garden/css
    [:.header
     {:display :grid
      :grid-template "'logo status spacer links' / 184px 100px 1fr 260px"
      :place-items "center start"
      :padding "12px 1rem"
      :width "100%"}
     [:&__logo
      {:display :flex
       :grid-area :logo
       :justify-content :space-between
       :align-items :center}]
     [:&__status
      {:grid-area :status}]
     [:&__display-mode-toggle
      {:display :none}]
     [:&__links
      {:display "flex"
       :grid-area :links
       :place-self "center stretch"
       :justify-content "space-between"
       :flex "0 0 250px"
       :align-items "center"}]]
    (gs/at-media {:max-width :1000px}
      [:.header
       {:padding "16px"
        :grid-gap :8px
        :grid-template "'logo status toggle' / 1fr 1fr 1fr"}
       [:&__display-mode-toggle
        {:display :block
         :justify-self :end
         :grid-area :toggle}]
       [:&__links
        {:display :none}]])
    (gs/at-media {:max-width :600px}
      [:.header
       {:padding "16px"
        :grid-template "'logo status toggle' / auto auto auto"}])
    (gs/at-media {:max-width :400px}
      [:.header
       {:padding "16px 4px 16px 16px"
        :grid-gap :4px
        :grid-template "'logo status toggle' / auto minmax(auto, 120px) auto"}
       [:&__status
        {:justify-self :stretch
         :max-width :100%}]]))])

(defn root []
  [:header.header
   header-styles
   [:div.header__logo {:on-click dispatch-second-layer-toggle}
    [css-logo/root]]
   [:div.header__status
    [node-status/node-status]]

   [:div.header__display-mode-toggle
    [comps/button-textual
     {:on-click disable-toggle-display-mode
      :text
      (if (= @-sub-display-mode :ui.display-mode/query)
        "To output >"
        "< To query")}]]

   [:div.header__links
    [:div.header__links__item
     [comps/link-outer cfg/url-docs "Docs"]]
    [:div.header__links__item
     [comps/link-outer cfg/url-chat "Crux Chat"]]
    [:div.header__links__item
     [comps/link-mailto cfg/url-mail]]]])

