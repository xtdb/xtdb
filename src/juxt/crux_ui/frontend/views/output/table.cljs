(ns juxt.crux-ui.frontend.views.output.table
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.style :as s]
            [garden.core :as garden]
            [reagent.core :as r]
            [juxt.crux-ui.frontend.logging :as log]))

(def ^:private style
  [:style
    (garden/css
      [:.q-grid
       {:border  s/q-ui-border
        :border-top :none
        :border-left :none
        :border-collapse :separate
        :border-radius :2px
        :display :block
        :width :100%
        :height :100%
        :position :relative}
       [:&--table
        {:display :table}
        [:>.q-grid__head
         :>.q-grid__body
         {:display :initial}]
        [:.q-grid__head-row
         :.q-grid__body-row
         {:display :table-row}]
        [:.q-grid__head-cell
         :.q-grid__body-cell
         {:display :table-cell}]]

       [:&__head
        {:display :block}
        [:&-row
         {:display :grid
          :grid-auto-flow :column
          :grid-auto-columns :1fr
          :border-bottom s/q-ui-border}]]

       ["&__body-cell"
        "&__head-cell"
         {:border-left s/q-ui-border
          :display :block
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
         :display :block
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
  [:tr.q-grid__body-row
   (for [c r]
     ^{:key c}
     [:td.q-grid__body-cell (and c (pr-str c))])])

(defn root [table-data]
  (let [instance-state
        (r/atom {:i/node nil
                 :i/ctrl-c? false})

        on-ku-internal
        (fn [evt]
          (when-let [meta-released? (#{"Meta" "Control"} (.-key evt))]
            (swap! instance-state assoc :i/ctrl-c? false)
            (.remove (.-classList (:i/node @instance-state)) "q-grid--table")))

        on-kd-internal
        (fn [kb-evt]
          (let [ctrl-or-cmd? (or (.-metaKey kb-evt) (.-ctrlKey kb-evt))
                c? (= "c" (.-key kb-evt))
                ctrl-c? (and ctrl-or-cmd? c?)]
            (when ctrl-c?
              (swap! instance-state assoc :i/ctrl-c? true)
              (.add (.-classList (:i/node @instance-state)) "q-grid--table"))))]

    (r/create-class
      {:component-will-unmount
       (fn [this]
         (js/window.removeEventListener "keydown" on-kd-internal true)
         (js/window.removeEventListener "keyup" on-ku-internal true))
       :component-did-mount
       (fn [this]
         (let [node (r/dom-node this)]
           (js/window.addEventListener "keydown" on-kd-internal true)
           (js/window.addEventListener "keyup" on-ku-internal true)
           (swap! instance-state assoc :i/node node)))

       :reagent-render
       (fn [{:keys [headers rows] :as table-data}]
        (let []
          [:table.q-grid
           style
           [:thead.q-grid__head
            [:th.q-grid__head-row
              (for [h headers]
                ^{:key h}
                [:td.q-grid__head-cell (pr-str h)])]]
           [:tbody.q-grid__body
            (map-indexed table-row rows)]]))})))


