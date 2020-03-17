(ns crux-ui.views.output.table
  (:require [re-frame.core :as rf]
            [crux-ui.views.style :as s]
            [garden.core :as garden]
            [reagent.core :as r]
            [crux-ui.functions :as f]
            [crux-ui.logging :as log]
            [crux-ui.views.commons.dom :as dom]))


(def ^:private table-style
  [:style
    (garden/css

      [:.g-cell-oversize-arrest
       [:&__content
        {:max-width  :300px
         :max-height :76px
         :overflow   :hidden}
        [:&--expanded
         {:max-width :none
          :max-height :none}]]
       [:&__expand
        {:text-align :center
         :cursor :pointer}]]

      [:.q-grid-wrapper
       {:overflow :scroll
        :height :100%
        :padding-bottom :15rem}
       ["> ::-webkit-scrollbar"
        "> ::-moz-scrollbar"
        "> ::scrollbar"
        {:display :none}]]
      [:.q-grid
       {:border-collapse :separate
        :border-radius :2px
        :width :100%
        :overflow :visible
        :position :relative}
       [:&__head
        {}
        [:&-row
         {}]]

       ["&__body-cell"
        "&__head-cell"
        {:border-left s/q-ui-border
         :border-bottom s/q-ui-border
         :padding "10px 12px"}]
       ["&__head-cell"
        {:border-top :none
         :background :white
         :position :sticky
         :top 0
         :text-align :center
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
         {:letter-spacing :.04em
          :border-bottom "1px solid hsl(200,20%, 90%)"}
         [:&:first-child
          {:border-left :none}]
         [:&--queryable
          {:cursor :pointer
           :box-shadow "inset 5px 0 0px -3px hsla(220, 90%, 70%, 0.5)"}
          [:&:hover
           {:box-shadow "inset 6px 0 0px -3px hsla(220, 90%, 70%, 1)"}]]]]])])

(defn- expand-sibling [click-evt]
  (let [cl (f/jsget click-evt "target" "previousSibling" "classList")]
    (^js .toggle cl "g-cell-oversize-arrest__content--expanded")))

(defn- on-queryable-cell-click [click-evt]
  (let [dataset (f/prefix-keys :data (dom/event->target-data click-evt))]
    (rf/dispatch [:evt.ui.query/submit--cell dataset])))

(defn- plain-value? [v]
  (or (boolean? v)
      (number? v)
      (keyword? v)
      (and (string? v) (< (.-length v) 100))))

; (pr-str (type true))

(defn table-row [attr-headers value-is-the-attribute? i row-item-map]
  ^{:key i}
  [:tr.q-grid__body-row
   (for [attr attr-headers
         :let [raw-value (get row-item-map attr)
               cell-content ^js/String (or (some-> raw-value pr-str) "")
               cont-length (.-length cell-content)
               queryable? (and raw-value (plain-value? raw-value))
               attr-str (pr-str (if value-is-the-attribute? raw-value attr))]]
     ^{:key attr}
     [:td.q-grid__body-cell

      (if queryable?
        (cond->
          {:class (if queryable? "q-grid__body-cell--queryable")
           :on-click (if queryable? on-queryable-cell-click)
           :title attr-str
           :data-idx i
           :data-attr attr-str}
          ;
          (not value-is-the-attribute?)
          (assoc :data-value (pr-str raw-value))))

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
       (fn [{:keys [headers attr-headers rows value-is-the-attribute?] :as table-data}]
         [:div.q-grid-wrapper
          [:table.q-grid
           table-style
           [:thead.q-grid__head
            [:tr.q-grid__head-row
              (for [[h ha] (map vector headers attr-headers)]
                ^{:key h}
                [:th.q-grid__head-cell {:title ha} (pr-str h)])]]
           [:tbody.q-grid__body
            (map-indexed (partial table-row attr-headers value-is-the-attribute?) rows)]]])})))


