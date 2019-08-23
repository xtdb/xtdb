(ns juxt.crux-ui.frontend.views.header
  (:require [garden.core :as garden]
            [garden.stylesheet :as gs]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.routes :as routes]
            [juxt.crux-ui.frontend.views.commons.tabs :as tabs]
            [juxt.crux-ui.frontend.views.commons.css-logo :as css-logo]
            [juxt.crux-ui.frontend.views.node-status :as node-status]
            [juxt.crux-ui.frontend.views.commons.input :as input]))

(def ^:private -tab-sub (rf/subscribe [:subs.ui/root-tab]))

(def ^:private header-styles
  [:style
   (garden/css
    [:.header
      {:display :grid
       :grid-template "'logo status spacer links' / 180px 100px 1fr 260px"
       :place-items "center start"
       :padding "12px 42px"
       :width "100%"}
      [:&__logo
        {:display :flex
         :grid-area :logo
         :justify-content :space-between
         :flex "0 0 200px"
         :align-items :center}]
      [:&__status
        {:grid-area :status}]
     [:&__links
      {:display "flex"
       :grid-area :links
       :place-self "center stretch"
       :justify-content "space-between"
       :flex "0 0 250px"
       :align-items "center"}]]
    (gs/at-media {:max-width :1000px}
      [:.header
       {:padding "16px"}
       [:&__links
        {:display :none}]]))])

(defn header-tabs []
  [:div.header__tabs
   [tabs/root
    {:active-tab-id   @-tab-sub
     :tabs
     [{:id :db.ui.root-tab/query-ui
       :href  (routes/path-for :rd/query-ui)
       :title "Query UI"}
      {:id    :db.ui.root-tab/settings
       :href  (routes/path-for :rd/settings)
       :title "Settings"}]}]])


(defn root []
  [:header.header
   header-styles
   [:div.header__logo
    [css-logo/root]]
   [:div.header__status
    [node-status/node-status]]

   [:div.header__links
    [:a.header__links__item {:href "https://juxt.pro/crux/docs/index.html"}
     [:span.n "Docs"]]
    [:a.header__links__item {:href "https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux"}
     [:span.n "Crux Chat"]]
    [:a.header__links__item {:href "mailto:crux@juxt.pro"}
     [:span.n "crux@juxt.pro"]]]])

