(ns juxt.crux-ui.frontend.views.query.results-table
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.style :as s]
            [garden.core :as garden]))

(def style
  (garden/css
    [:.q-table
     {:border  s/q-ui-border
      :border-top :none
      :border-left :none
      :border-collapse :separate
      :border-radius :2px
      :width :100%
      :position :relative}

     ["&__body-cell"
      "&__head-cell"
       {:border-left s/q-ui-border
        :border-top  s/q-ui-border
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

(defn table-row [i r]
  ^{:key i}
  [:tr.q-table__body-row
   (for [c r]
     ^{:key c}
     [:td.q-table__body-cell (and c (pr-str c))])])

(defn root [table-data]
  (let [{:keys [headers rows]} table-data]
    [:table.q-table
     [:style style]
     [:thead.q-table__head
      [:tr.q-table__head-row
       (for [h headers]
         ^{:key h}
         [:th.q-table__head-cell (pr-str h)])]]
     [:tbody.q-table__body
      (map-indexed table-row rows)]]))


