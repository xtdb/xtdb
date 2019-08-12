(ns juxt.crux-ui.frontend.views.query.output
  (:require [garden.core :as garden]
            [garden.stylesheet :as gs]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.comps :as comps]
            [juxt.crux-ui.frontend.views.output.tree :as q-results-tree]
            [juxt.crux-ui.frontend.views.output.tx-history :as output-txes]
            [juxt.crux-ui.frontend.views.output.attr-history :as output-attr-history]
            [juxt.crux-ui.frontend.views.output.edn :as output-edn]
            [juxt.crux-ui.frontend.views.output.error :as q-err]
            [juxt.crux-ui.frontend.views.output.table :as q-results-table]
            [juxt.crux-ui.frontend.views.style :as s]
            [juxt.crux-ui.frontend.views.attr-stats :as attr-stats]
            [juxt.crux-ui.frontend.views.codemirror :as cm]))


(def ^:private -sub-err             (rf/subscribe [:subs.query/error-improved]))
(def ^:private -sub-query-res-raw   (rf/subscribe [:subs.query/result]))
(def ^:private -sub-output-tab      (rf/subscribe [:subs.ui/output-main-tab]))
(def ^:private -sub-output-side-tab (rf/subscribe [:subs.ui/output-side-tab]))
(def ^:private -sub-results-table   (rf/subscribe [:subs.query/results-table]))


(def empty-placeholder
  [:div.q-output-empty "Your query or transaction results here shall be"])

(defn set-side-tab [tab-name]
  (rf/dispatch [:evt.ui.output/side-tab-switch tab-name]))

(defn set-main-tab [tab-name]
  (rf/dispatch [:evt.ui.output/main-tab-switch tab-name]))


(def ^:private q-output-tabs-styles
  [:style
    (garden/css
      [:.output-tabs
       {:display :flex}
       [:&__item
        {:ee 3}]
       [:&__sep
        {:padding "0 8px"}]])])

(defn out-tab-item [tab active-tab on-click]
  [comps/button-textual
   {:on-click on-click :active? (= tab active-tab) :text (name tab)}])


(defn side-output-tabs [active-tab]
  [:div.output-tabs.output-tabs--side
   q-output-tabs-styles
   (->>
     (for [tab-type [:db.ui.output-tab/tree :db.ui.output-tab/attr-stats]]
         [out-tab-item tab-type active-tab #(set-side-tab tab-type)])
     (interpose [:div.output-tabs__sep "/"])
     (map-indexed #(with-meta %2 {:key %1})))])


(defn main-output-tabs [active-tab]
  [:div.output-tabs.output-tabs--main
   q-output-tabs-styles
   (->>
     (for [tab-type [:db.ui.output-tab/table
                     :db.ui.output-tab/tree
                     :db.ui.output-tab/attr-history
                     :db.ui.output-tab/tx-history
                     :db.ui.output-tab/edn]]
       ^{:key tab-type}
       [out-tab-item tab-type active-tab #(set-main-tab tab-type)])
     (interpose [:div.output-tabs__sep "/"])
     (map-indexed #(with-meta %2 {:key %1})))])



(def ^:private q-output-styles
  [:style
    (garden/css
      [:.q-output
       {:border "0px solid red"
        :height :100%
        :display :grid
        :position :relative
        :grid-template "'side main' / minmax(280px, 400px) 1fr"}
       [:&__side
        {:border-right s/q-ui-border
         :grid-area :side
         :overflow :hidden
         :position :relative}
        [:&__content
         {:overflow :auto
          :height :100%}]]
       [:&__main
        {:border-radius :2px
         :grid-area :main
         :position :relative
         :overflow :hidden}]
       ["&__main__links"
        "&__side__links"
        {:position :absolute
         :z-index 10
         :background :white
         :padding :8px
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
      (gs/at-media {:max-width :1000px}
        [:.q-output
         {:grid-template "'main main' 1fr / minmax(280px, 300px) 1fr"}
         [:&__side
          {:display :none}]]))])

(defn root []
  (let [main-tab @-sub-output-tab]
    [:div.q-output
     q-output-styles

     [:div.q-output__side
      (let [out-tab @-sub-output-side-tab]
        [:<>
          [:div.q-output__side__content
              (case out-tab
                :db.ui.output-tab/attr-stats [attr-stats/root]
                :db.ui.output-tab/empty empty-placeholder
                [q-results-tree/root])]
          [:div.q-output__side__links
           [side-output-tabs out-tab]]])]

     [:div.q-output__main
      (if main-tab
        [:<>
         (case main-tab
           :db.ui.output-tab/error          [q-err/root @-sub-err]
           :db.ui.output-tab/table          [q-results-table/root @-sub-results-table]
           :db.ui.output-tab/tree           [q-results-tree/root]
           :db.ui.output-tab/tx-history     [output-txes/root]
           :db.ui.output-tab/attr-history   [output-attr-history/root]
           :db.ui.output-tab/edn            [output-edn/root @-sub-query-res-raw]
           :db.ui.output-tab/empty          empty-placeholder
           [q-results-table/root @-sub-results-table])
         [:div.q-output__main__links
          [main-output-tabs main-tab]]]
        empty-placeholder)]]))
