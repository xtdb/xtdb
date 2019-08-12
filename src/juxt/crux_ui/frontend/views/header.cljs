(ns juxt.crux-ui.frontend.views.header
  (:require [garden.core :as garden]
            [garden.stylesheet :as gs]
            [juxt.crux-ui.frontend.views.commons.tabs :as tabs]
            [re-frame.core :as rf]))

(def ^:private -tab-sub (rf/subscribe [:subs.ui/root-tab]))

(def header-styles
  [:style
   (garden/css
    [:.header
      {:display :flex
       :justify-content :space-between
       :align-items :center
       :padding "16px 42px"
       :width "100%"}
      [:&__logo
        {:display :flex
         :justify-content :space-between
         :flex "0 0 200px"
         :align-items :center}]
      [:&__links
       {:display "flex"
        :justify-content "space-between"
        :flex "0 0 250px"
        :align-items "center"}]]
    (gs/at-media {:max-width :1000px}
      [:.header
       {:padding "16px"}
       [:&__links
        {:display :none}]]))])

(defn on-tab-activate [tab-id]
  (rf/dispatch [:evt.ui/root-tab-switch tab-id]))

(defn root []
  [:header.header
   header-styles
   [:div.header__logo
     [:div.logo [:a {:href "/"}
                 [:img.logo-img {:width 200 :src "/static/img/console-logo.svg"}]]]]
   [:div.header__tabs
     [tabs/root
      {:on-tab-activate on-tab-activate
       :active-tab-id @-tab-sub
       :tabs
        [{:id :db.ui.root-tab/query-ui
          :title "Query UI"}
         {:id :db.ui.root-tab/settings
          :title "Settings"}]}]]

   [:div.header__links
    [:a.header__links__item {:href "https://juxt.pro/crux/docs/index.html"}
     [:span.n "Docs"]]
    [:a.header__links__item {:href "https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux"}
     [:span.n "Crux Chat"]]
    [:a.header__links__item {:href "mailto:crux@juxt.pro"}
     [:span.n "crux@juxt.pro"]]]])

