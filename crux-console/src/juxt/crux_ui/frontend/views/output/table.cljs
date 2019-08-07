(ns juxt.crux-ui.frontend.views.output.table
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.style :as s]
            [garden.core :as garden]))

(def ^:private style
  [:style
    (garden/css
      [:.q-grid
       {:border  s/q-ui-border
        :border-top :none
        :border-left :none
        :border-collapse :separate
        :border-radius :2px
        :width :100%
        :height :100%
        :position :relative}

       [:&__head
        {:display :grid
         :grid-auto-flow :column
         :grid-auto-columns :1fr
         :border-bottom s/q-ui-border}]

       ["&__body-cell"
        "&__head-cell"
         {:border-left s/q-ui-border
          :padding "6px 12px"}]
       ["&__head-cell"
        {:border-top :none
         :background :white
         :text-align :center
         :font-weight 400
         :letter-spacing :.09em}
        [:&:first-child
         {:border-left :none}]]
       [:&__body
        {:overflow :auto
         :height :100%}
        [:&-row
         {:display :grid
          :border-top  s/q-ui-border
          :grid-auto-flow :column
          :grid-auto-columns :1fr}
         [:&:first-child
          {:border-top  :none}]]
        [:&-cell
         {:letter-spacing :.04em}
         [:&:first-child
          {:border-left :none}]]]])])

(defn table-row [i r]
  ^{:key i}
  [:div.q-grid__body-row
   (for [c r]
     ^{:key c}
     [:div.q-grid__body-cell (and c (pr-str c))])])

(defn root [table-data]
  (let [{:keys [headers rows]} table-data]
    [:div.q-grid
     style
     [:div.q-grid__head
      (for [h headers]
        ^{:key h}
        [:div.q-grid__head-cell (pr-str h)])]
     [:div.q-grid__body
      (map-indexed table-row rows)]]))


