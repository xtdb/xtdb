(ns juxt.crux-ui.frontend.views.query.results-table
  (:require [re-frame.core :as rf]
            [garden.core :as garden]))

(def ^:private -sub-results-table (rf/subscribe [:subs.query/results-table]))
(def col-border "hsl(0, 0%, 85%)")
(def border (str "1px solid " col-border))

(def style
  (garden/css
    [:.q-table
     {:border  border
      :border-top :none
      :border-left :none
      :border-collapse :separate
      :border-radius :2px
      :width :100%
      :position :relative}

     ["&__body-cell"
      "&__head-cell"
       {:border-left border
        :border-top  border
        :padding "6px 12px"}]
     ["&__head-cell"
      {:border-top :none
       :background :white
       :font-weight 400
       :letter-spacing :.09em}
      [:&:first-child
       {:border-left :none}]]
     ["&__body-cell"
      {:letter-spacing :.04em}
      [:&:first-child
       {:border-left :none}]]]))


(defn root []
  (let [{:keys [headers rows]} @-sub-results-table]
    [:table.q-table
     [:style style]
     [:thead.q-table__head
      [:tr.q-table__head-row
       (for [h headers]
         [:th.q-table__head-cell (pr-str h)])]]
     [:tbody.q-table__body
      (for [r rows]
        [:tr.q-table__body-row
         (for [c r]
           [:td.q-table__body-cell (and c (pr-str c))])])]]))
