(ns juxt.crux-ui.frontend.views.output.table
  (:require [re-frame.core :as rf]
            [juxt.crux-ui.frontend.views.style :as s]
            [garden.core :as garden]
            [reagent.core :as r]
            [juxt.crux-ui.frontend.functions :as f]
            [juxt.crux-ui.frontend.logging :as log]))


(def ^:private table-style
  [:style
    (garden/css

      [:.g-cell-oversize-arrest
       [:&__content
        {:max-width :300px
         :max-height :76px
         :overflow :hidden}
        [:&--expanded
         {:max-width :none
          :max-height :none}]]
       [:&__expand
        {:text-align :center
         :cursor :pointer}]]

      [:.q-grid-wrapper
       {:overflow :scroll
        :padding-bottom :15rem}
       ["> ::-webkit-scrollbar"
        "> ::-moz-scrollbar"
        "> ::scrollbar"
        {:display :none}]]
      [:.q-grid
       {:border-collapse :collapse
        :border-radius :2px
        :width :100%
        :height :100%
        :overflow :visible
        :position :relative}
       [:&__head
        {}
        [:&-row
         {:border-bottom s/q-ui-border}]]

       ["&__body-cell"
        "&__head-cell"
         {:border-left s/q-ui-border
          :padding "6px 12px"}]
       ["&__head-cell"
        {:border-top :none
         :background :white
         :position :sticky
         :top 0
         :text-align :center
         :border-bottom s/q-ui-border
         :font-weight 400
         :letter-spacing :.10em}
        [:&:first-child
         {:border-left :none}]]
       [:&__body
        [:&-row
         {:border-top  s/q-ui-border}
         [:&:first-child
          {:border-top  :none}]]
        [:&-cell
         {:letter-spacing :.04em}
         [:&:first-child
          {:border-left :none}]]]])])

(defn- expand-sibling [click-evt]
  (let [cl (f/jsget click-evt "target" "previousSibling" "classList")]
    (^js .toggle cl "g-cell-oversize-arrest__content--expanded")))

(defn table-row [headers i row-items]
  ^{:key i}
  [:tr.q-grid__body-row
   (for [j (range (count row-items))
         :let [cell-content ^js/String (or (some-> (nth row-items j) pr-str) "")
               cont-length (.-length cell-content)
               cell-id (nth headers j)]]
     ^{:key cell-id}
     [:td.q-grid__body-cell
      (if (> cont-length 100)
        [:div.g-cell-oversize-arrest
         [:div.g-cell-oversize-arrest__content cell-content]
         [:div.g-cell-oversize-arrest__expand
          {:on-click expand-sibling} "..."]]
        cell-content)])])

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
         [:div.q-grid-wrapper
          [:table.q-grid
           table-style
           [:thead.q-grid__head
            [:tr.q-grid__head-row
              (for [h headers]
                ^{:key h}
                [:th.q-grid__head-cell (pr-str h)])]]
           [:tbody.q-grid__body
            (map-indexed (partial table-row headers) rows)]]])})))


