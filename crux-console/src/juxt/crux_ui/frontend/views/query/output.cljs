(ns juxt.crux-ui.frontend.views.query.output
  (:require [garden.core :as garden]
            [garden.stylesheet :as gs]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.comps :as comps]
            [juxt.crux-ui.frontend.views.output.react-tree :as q-results-tree]
            [juxt.crux-ui.frontend.views.output.tx-history :as output-txes]
            [juxt.crux-ui.frontend.views.output.attr-history :as output-attr-history]
            [juxt.crux-ui.frontend.views.output.edn :as output-edn]
            [juxt.crux-ui.frontend.views.output.error :as q-err]
            [juxt.crux-ui.frontend.views.commons.cube-svg :as cube]
            [juxt.crux-ui.frontend.views.output.table :as q-results-table]
            [juxt.crux-ui.frontend.views.style :as s]
            [juxt.crux-ui.frontend.views.attr-stats :as attr-stats]
            [juxt.crux-ui.frontend.views.codemirror :as cm]
            [reagent.core :as r]
            [juxt.crux-ui.frontend.routes :as routes]
            [juxt.crux-ui.frontend.views.commons.tabs :as tabs]))


(def ^:private -sub-err             (rf/subscribe [:subs.query/error-improved]))
(def ^:private -sub-query-res-raw   (rf/subscribe [:subs.query/result]))
(def ^:private -sub-result-tree     (rf/subscribe [:subs.query/result-tree]))
(def ^:private -sub-output-tab      (rf/subscribe [:subs.ui/output-main-tab]))
(def ^:private -sub-nw-progress     (rf/subscribe [:subs.query/network-in-progress?]))
(def ^:private -sub-output-side-tab (rf/subscribe [:subs.ui/output-side-tab]))
(def ^:private -sub-results-table   (rf/subscribe [:subs.query/results-table]))


(def empty-placeholder
  [:div.q-output-empty "Your query or transaction results here shall be"])

(defn set-main-tab [tab-name]
  (rf/dispatch [:evt.ui.output/main-tab-switch tab-name]))

(def ^:private -sub-mobile-mode (rf/subscribe [:subs.ui.responsive-breakpoints/width-lt-800]))

(def desktop-tabs
  [{:tabs/title "table"
    :tabs/id :db.ui.output-tab/table}
   {:tabs/title "tree"
    :tabs/id :db.ui.output-tab/tree}
   {:tabs/title "attribute frequencies"
    :tabs/id :db.ui.output-tab/attr-stats}
   {:tabs/title "attribute history"
    :tabs/id :db.ui.output-tab/attr-history}
   {:tabs/title "transactions"
    :tabs/id :db.ui.output-tab/tx-history}
   {:tabs/title "edn output"
    :tabs/id :db.ui.output-tab/edn}])

(def mobile-tabs
  [{:tabs/title "Table"
    :tabs/id :db.ui.output-tab/table}
   {:tabs/title "Tree"
    :tabs/id :db.ui.output-tab/tree}
   {:tabs/title "Attr stats"
    :tabs/id :db.ui.output-tab/attr-stats}
   {:tabs/title "Attr history"
    :tabs/id :db.ui.output-tab/attr-history}
   {:tabs/title "Txes"
    :tabs/id :db.ui.output-tab/tx-history}
   {:tabs/title "EDN"
    :tabs/id :db.ui.output-tab/edn}])

(defn main-output-tabs [active-tab]
  [:div.output-tabs.output-tabs--main
   ^{:key active-tab}
   [tabs/root
    {:tabs/active-id active-tab
     :tabs/on-tab-activate set-main-tab
     :tabs/tabs (if -sub-mobile-mode mobile-tabs desktop-tabs)}]])



(def ^:private q-output-styles
  [:style
    (garden/css
      [:.q-output
       {:border "0px solid red"
        :height :100%
        :display :grid
        :position :relative
        :overflow :hidden
        :padding-bottom :20px
        :grid-template "'main' / 1fr"}
       [:&__side
        {:border-right s/q-ui-border
         :grid-area :side
         :overflow :hidden
         :position :relative}
        [:&__content
         {:overflow :auto
          :height :100%}]]
       [:&__tabs
        {:position :absolute
         :max-width :100%
         :overflow :auto
         :z-index 10
         :background :white
         :bottom  :0px
         :right   :0px}]]
      [:.q-output-edn
       {:padding :8px}]
      [:.q-output-empty
       {:height :100%
        :display :flex
        :color s/color-placeholder
        :align-items :center
        :justify-content :center}]

      [:.q-preloader
       {:width :100%
        :height :100%
        :display :flex
        :align-items :center
        :justify-content :center}
       [:&__cube
        {:width :200px
         :height :200px}]]

      [:.output-tabs
       {:width :max-content
        :padding :8px}]

      (gs/at-media {:max-width :1000px}
        [:.q-output
         {:grid-template "'main' 1fr / 1fr"
          :padding-bottom :4rem}
         [:&__tabs
          {:bottom :1.5rem}]])

      (gs/at-media {:max-width :375px}
        [:.q-output
         {:grid-template "'main' 1fr / 1fr"
          :padding-bottom :4rem}
         [:&__tabs
          {:bottom :1.5rem}]])
      )])


(defn- output-preloader []
  [:div.q-preloader
   [:div.q-preloader__cube
    [cube/cube {:animating? true}]]])

(defn root []
    [:div.q-output
     q-output-styles
     (let [main-tab @-sub-output-tab]
       (cond
         @-sub-nw-progress [output-preloader]
         main-tab [:<>
                   (case main-tab
                     :db.ui.output-tab/error          [q-err/root @-sub-err]
                     :db.ui.output-tab/table          [q-results-table/root @-sub-results-table]
                     :db.ui.output-tab/tree           [q-results-tree/root @-sub-result-tree]
                     :db.ui.output-tab/tx-history     [output-txes/root]
                     :db.ui.output-tab/attr-history   [output-attr-history/root]
                     :db.ui.output-tab/attr-stats     [attr-stats/root]
                     :db.ui.output-tab/edn            [output-edn/root @-sub-query-res-raw]
                     :db.ui.output-tab/empty          empty-placeholder
                     [q-results-table/root @-sub-results-table])
                   [:div.q-output__tabs
                    [main-output-tabs main-tab]]]
         :else empty-placeholder))])
