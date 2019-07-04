(ns juxt.crux-ui.frontend.views.query.output
  (:require [juxt.crux-ui.frontend.views.comps :as comps]
            [juxt.crux-ui.frontend.views.query.results-tree :as q-results-tree]
            [juxt.crux-ui.frontend.views.query.results-table :as q-results-table]
            [garden.core :as garden]
            [garden.stylesheet :as gs]
            [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.codemirror :as cm]))


(def ^:private -sub-query-res (rf/subscribe [:subs.query/result]))
(def ^:private -sub-output-tab (rf/subscribe [:subs.ui/output-tab]))
(def color-placeholder :grey)

(def q-ui-border "1px solid hsl(0,0%,85%)")

(defn- query-output-edn []
  (let [raw @-sub-query-res
        fmt (with-out-str (cljs.pprint/pprint raw))]
    [cm/code-mirror fmt {:read-only? true}]))


(def empty-placeholder
  [:div.q-output-empty "Your query or transaction results here shall be"])

(defn set-tab [tab-name]
  #(rf/dispatch [:evt.ui.output/tab-switch tab-name]))


(def ^:private q-output-tabs-styles
  [:style
    (garden/css
      [:.output-tabs
       {:display :flex}
       [:&__item
        {}]
       [:&__sep
        {:padding "0 8px"}]])])

(defn out-tab-item [tab active-tab]
  [comps/button-textual
   {:on-click (set-tab tab) :active? (= tab active-tab) :text (name tab)}])


(defn output-tabs [active-tab]
  [:div.output-tabs
   q-output-tabs-styles
   [out-tab-item :db.ui.output-tab/table active-tab]
   [:div.output-tabs__sep "/"]
   [out-tab-item :db.ui.output-tab/tree active-tab]
   [:div.output-tabs__sep "/"]
   [out-tab-item :db.ui.output-tab/edn active-tab]])


(def ^:private q-output-styles
  [:style
    (garden/css
      [:.q-output
       {:border "0px solid red"
        :height :100%
        :display :grid
        :position :relative
        :grid-template "'side main' 1fr / minmax(200px, 300px) 1fr"}
       [:&__side
        {:border-right q-ui-border
         :grid-area :side
         :overflow :auto}]
       [:&__main
        {:border-radius :2px
         :grid-area :main
         :overflow :auto}
        [:&__links
         {:position :absolute
          :z-index 10
          :background :white
          :padding :8px
          :bottom :0px
          :right  :0px}]]]
      [:.q-output-edn
       {:padding :8px}]
      [:.q-output-empty
       {:height :100%
        :display :flex
        :color color-placeholder
        :align-items :center
        :justify-content :center}]
      (gs/at-media {:max-width :1000px}
        [:.q-output
         {:grid-template "'main main' 1fr / minmax(200px, 300px) 1fr"}
         [:&__side
          {:display :none}]]))])

(defn root []
  [:div.q-output
   q-output-styles
   [:div.q-output__side
     [q-results-tree/root]]
   [:div.q-output__main
    (if-let [out-tab @-sub-output-tab]
      [:<>
       (case out-tab
         :db.ui.output-tab/table [q-results-table/root]
         :db.ui.output-tab/tree  [q-results-tree/root]
         :db.ui.output-tab/edn   [query-output-edn]
         :db.ui.output-tab/empty empty-placeholder
         [q-results-table/root])
       [:div.q-output__main__links
         [output-tabs out-tab]]]
       empty-placeholder)]])
