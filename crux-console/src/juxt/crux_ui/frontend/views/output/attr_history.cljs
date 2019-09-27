(ns juxt.crux-ui.frontend.views.output.attr-history
  (:require [re-frame.core :as rf]
            [garden.core :as garden]
            [juxt.crux-ui.frontend.views.charts.wrapper-basic :as charts]
            [juxt.crux-ui.frontend.functions :as f]
            [juxt.crux-ui.frontend.views.commons.tiny-components  :as comps]))

(def ^:private -sub-plotly-data (rf/subscribe [:subs.query/attr-history-plot-data]))


(def ^:private root-styles
  [:style
   (garden/css
     [:.attr-history
      {:height :100%
       :padding "6px 8px"
       :position :relative}
      [:&__chart
       {:height :100%}]
      [:&__hint
       {:position :absolute
        :background "hsla(0,0%, 100%, 0.9)"
        :top 0
        :left 0
        :bottom 0
        :right 0
        :overflow :hidden
        :display :grid
        :grid-template
        (f/lines
          "'h h h' 1fr"
          "'l hint r' auto"
          "'l btn r' auto"
          "'f f f' 1fr"
          "/ 1fr auto 1fr")
        :grid-gap :32px
        :place-items "center start"
        :width :100%
        :height :100%}
       ["> .q-output-empty"
        {:height :auto
         :grid-area :hint}]
       ["> .button"
        {:height :auto
         :grid-area :btn}]]])])

(def ^:private hint
  (str "This view composes `history-range` and `documents` endpoints to\n"
       "give you a some understanding of how an entity attribute has changed over valid time.\n"
       "This view is limited to display up to 7 entities.\n"
       "You can use \"put with valid time\" example to transact in\n"
       "some historical data for. Use that entity id later in your query.\n"
       "\n"
       "Try to run a query that will include a numeric attribute like:\n"
       "{:find [e p]\n"
       " :args [{ids #{:ids/tech-ticker-2 :ids/pharma-ticker-5}}]\n"
       " :where\n"
       " [[e :crux.db/id]\n"
       "  [e :ticker/price p]\n"
       "  [(contains? ids e)]]}\n"))

(defn- attr-layout [attr]
  {:title (str (pr-str attr) " over time")
   :xaxis {:title "Valid Time"}
   :yaxis {:title (pr-str attr)}})

(defn- dispatch-disable-hint [_]
  (rf/dispatch [:evt.ui.attr-history/disable-hint]))

(defn root []
  [:div.attr-history
   root-styles
   (let [{:keys [traces attribute hint?] :as p-data} @-sub-plotly-data]
     (if p-data
       [:<>
        [:div.attr-history__chart
         [charts/plotly-wrapper traces (attr-layout attribute)]]
        (if hint?
          [:div.attr-history__hint
           [:pre.q-output-empty hint]
           [comps/button-bordered
            {:on-click dispatch-disable-hint
             :text "Got it"}]])]
       [:pre.q-output-empty hint]))])


