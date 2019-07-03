(ns juxt.crux-ui.frontend.views.header
  (:require [garden.core :as garden]
            [garden.stylesheet :as gs]))


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


(def tabs-styles
  [:style (garden/css
    [:.tabs
      {:display "flex"
       :justify-content "space-between"
       :font-size "20px"
       :align-items "center"}
      [:&__sep
        {:padding "16px"}]])])


(defn tabs []
  [:div.tabs
   tabs-styles
   [:div.tabs__item [:b "Query UI"]]
   #_[:div.tabs__sep "/"]
   #_[:div.tabs__item "Cluster"]])

(defn root []
  [:header.header
   header-styles
   [:div.header__logo
     [:div.logo [:a {:href "/"}
                 [:img.logo-img {:width 200 :src "/static/img/console-logo.svg"}]]]]
   [:div.header__tabs
     [tabs]]
   [:div.header__links
    [:a.header__links__item {:href "https://juxt.pro/crux/docs/index.html"}
     [:span.n "Docs"]]
    [:a.header__links__item {:href "https://juxt-oss.zulipchat.com/#narrow/stream/194466-crux"}
     [:span.n "Crux Chat"]]
    [:a.header__links__item {:href "mailto:crux@juxt.pro"}
     [:span.n "crux@juxt.pro"]]]])

