(ns juxt.crux-ui.frontend.views.query.output
  (:require [garden.core :as garden]
            [garden.stylesheet :as gs]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.commons.tiny-components  :as comps]
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
(def ^:private -sub-result-raw      (rf/subscribe [:subs.query/result]))
(def ^:private -sub-result-count    (rf/subscribe [:subs.query/result-count]))
(def ^:private -sub-result-tree     (rf/subscribe [:subs.query/result-tree]))
(def ^:private -sub-output-tab      (rf/subscribe [:subs.ui/output-main-tab]))
(def ^:private -sub-nw-progress     (rf/subscribe [:subs.query/network-in-progress?]))
(def ^:private -sub-results-table   (rf/subscribe [:subs.query/results-table]))
(def ^:private -sub-mobile-mode     (rf/subscribe [:subs.ui.responsive-breakpoints/width-lt-800]))


(def empty-placeholder
  [:pre.q-output-empty
   "Results will be displayed here.\n"
   "Example query:\n"
   "{:find [e p]\n"
   " :where\n"
   " [[e :ticker/price p]\n"
   "  [(> p 50)]]}"])

(def empty-results
  [:pre.q-output-empty "Query succeeded! Zero results."])


(defn set-main-tab [tab-name]
  (rf/dispatch [:evt.ui.output/main-tab-switch tab-name]))

(defn- tab [title tab-id]
  {:tabs/title title
   :tabs/href  (routes/path-for-tab tab-id {:r/search js/location.search})
   :tabs/id    tab-id})

(defn- desktop-tabs []
  [(tab "table" :db.ui.output-tab/table)
   (tab "tree" :db.ui.output-tab/tree)
   (tab "attribute frequencies" :db.ui.output-tab/attr-stats)
   (tab "attribute history" :db.ui.output-tab/attr-history)
   (tab "transactions" :db.ui.output-tab/tx-history)
   (tab "edn output" :db.ui.output-tab/edn)])

(defn- mobile-tabs []
  [(tab "Table" :db.ui.output-tab/table)
   (tab "Tree" :db.ui.output-tab/tree)
   (tab "Attr stats" :db.ui.output-tab/attr-stats)
   (tab "Attr history" :db.ui.output-tab/attr-history)
   (tab "Txes" :db.ui.output-tab/tx-history)
   (tab "EDN" :db.ui.output-tab/edn)])

(defn main-output-tabs [active-tab]
  [:div.output-tabs.output-tabs--main
   ^{:key active-tab}
   [tabs/root
    {:tabs/active-id active-tab
     :tabs/on-tab-activate set-main-tab
     :tabs/tabs
     (if @-sub-mobile-mode
       (mobile-tabs)
       (desktop-tabs))}]])



(def ^:private q-output-styles
  [:style
    (garden/css
      [:.q-output
       {:border "0px solid red"
        :height :100%
        :display :grid
        :position :relative
        :overflow :hidden
        :grid-template
        "'pane' calc(100% - 40px)
         'status' auto
         / 1fr"}
       [:&__pane
        {:grid-area :pane
         :overflow :hidden}]
       [:&__status
        {:grid-area :status
         :max-width :100%
         :overflow :auto
         :z-index 10
         :display :flex
         :justify-content :space-between
         :align-items :center
         :background :white
         :bottom  :0px
         :right   :0px
         :left          :0px
         :padding-left  :12px
         :padding-right :12px}
        [:&__result-count
         {:margin-right :8px
          :white-space :pre}]]]

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
       {; :width :max-content
        :padding "8px 0"}]

      (gs/at-media {:max-width :768px}
        [:.q-output
         ; add padding on mobile, cause iphones have a thumb on the bottom
         {:padding-bottom :20px}
         [:&__status
          {:padding-right :12px}]])

      (gs/at-media {:max-width :375px}
        [:.q-output
         {:padding-bottom :20px}]))])



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
                   [:div.q-output__pane
                    (case main-tab
                      :db.ui.output-tab/error          [q-err/root @-sub-err]
                      :db.ui.output-tab/table          [q-results-table/root @-sub-results-table]
                      :db.ui.output-tab/tree           [q-results-tree/root @-sub-result-tree]
                      :db.ui.output-tab/tx-history     [output-txes/root]
                      :db.ui.output-tab/attr-history   [output-attr-history/root]
                      :db.ui.output-tab/attr-stats     [attr-stats/root]
                      :db.ui.output-tab/edn            [output-edn/root @-sub-result-raw]
                      :db.ui.output-tab/empty          empty-placeholder
                      :db.ui.output-tab/empty-result   empty-results
                      [q-results-table/root @-sub-results-table])]
                   [:div.q-output__status
                    [:div.q-output__status__result-count @-sub-result-count]
                    [:div.q-output__status__tabs [main-output-tabs main-tab]]]]
         :else empty-placeholder))])
