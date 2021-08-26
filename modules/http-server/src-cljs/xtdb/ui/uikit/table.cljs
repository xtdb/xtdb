(ns xtdb.ui.uikit.table
  (:require
   [clojure.pprint :as pprint]
   [re-frame.core :as rf]
   [xtdb.ui.events :as events]
   [xtdb.ui.subscriptions :as sub]
   [xtdb.ui.uikit.utils :as utils]
   [reagent.core :as r]))

(defn pprint-str
  [x]
  (with-out-str (pprint/pprint x)))

(defn pprint-state
  [x]
  [:code
   {:style {:text-align "left"}}
   [:pre (pprint-str x)]])

(defn column-filter-select
  [data table-atom column-key]
  (let [normalized-value (-> data :filters :select-normalize column-key)]
    [:div.column__filter
     [utils/component-hide-show
      (fn [active? ref-toggle ref-box]
        [:div.column__button-wrapper
         [:button.button.column__button
          {:ref ref-toggle}
          [:i.column__button-icon.fas.fa-filter]
          [:span "Select"]
          [:i.column__button-icon.fas.fa-chevron-down]
          [:div.column__button-options
           {:class (when active? "column__button-options--show")
            :ref ref-box}
           (doall
            (for [[id value processed-value] (utils/column-select-filter-options data column-key)]
              ^{:key (str id value)}
              [:div.action__checkbox
               {:on-click #(utils/column-select-filter-on-change table-atom column-key value processed-value)}
               [:input
                {:type "checkbox"
                 :checked (utils/column-select-filter-value @table-atom column-key value processed-value)
                 :value value
                 :on-change #()}]
               [:label (if normalized-value
                         value processed-value)]]))]]])]]))

(defn column-filter-input
  [table-atom column-key]
  [:div.column__filter
   [:input.input.input--side-icons.input__no-borders
    {:value (utils/column-filter-value @table-atom column-key)
     :on-change #(utils/column-filter-on-change % table-atom column-key)}]
   [:span.input__icon.input__left-icon
    [:i.fas.fa-filter]]
   (when (not-empty (utils/column-filter-value @table-atom column-key))
     [:span.input__icon.input__right-icon.input__icon--clickable
      {:on-click #(utils/column-filter-reset table-atom column-key)}
      [:i.fas.fa-times]])])

(defn header-columns
  [data table-atom]
  (let [columns (utils/table-columns data @table-atom)]
    [:thead.table__head
     (into [:tr
            [:th.row-number.head__cell
             [:span "#"]]]
           (map
            (fn [{:keys [column-key column-name]}]
              ^{:key column-key}
              [:th.table__cell.head__cell
               [:div.head__column-title
                [:span column-name]]
               (case (utils/column-filter-type data column-key)
                 :input [column-filter-input table-atom column-key]
                 :select [column-filter-select data table-atom column-key]
                 [:div.column__filter--disabled])])
            columns))]))

(defn loading-table
  [{:keys [rows cols]}]
  [:div.table__main.table__main--loading
   [:table.table
    [:thead
     [:tr
      (for [col (range cols)]
        ^{:key col}
        [:th [:span]])]]
    [:tbody.table__body
     (for [row (range rows)]
       ^{:key row}
       [:tr.loading
        (for [col (range cols)]
          ^{:key col}
          [:td.td-loading-bar
           [:span.loading-bar__span]])])]]])

(defn body-rows
  [data table-atom rows]
  (let [columns (utils/table-columns data @table-atom)]
    [:tbody.table__body
     (for [[index row] (map-indexed vector rows)]
       ^{:key (str row index)}

       [:<>
        [:tr.table__row.body__row
         [:td.row-number (+ 1 index)]
         (for [{:keys [column-key render-fn]} columns]
           ^{:key (str row column-key)}
           [:td.table__cell.body__cell
            (if render-fn
              (render-fn row (column-key row))
              (column-key row))])]])]))

(defn actions
  [data table-atom]
  [:div.top__actions
   [utils/component-hide-show
    (fn [active? ref-toggle ref-box]
      [:div.action
       [:i.action__icon.fas.fa-th-large
        {:ref ref-toggle}]
       (into
        [:div.action__options
         {:class (when active? "action__options--show")
          :ref ref-box}
         [:div.action__title
          "Columns"]]
        (map
         (fn [{:keys [column-key column-name]}]
           ^{:key column-key}
           [:div.action__checkbox.switch__group
            {:on-click #(utils/column-visibility-on-change table-atom column-key)}
            [:div.onoffswitch
            [:input
             {:name "onoffswitch"
              :class "onoffswitch-checkbox"
              :id column-key
              :checked (utils/column-visible? @table-atom column-key)
              :on-change #(utils/column-visibility-on-change table-atom column-key)
              :type "checkbox"}]
             [:label.onoffswitch-label
              {:for column-key}
             [:span.onoffswitch-inner]
             [:span.onoffswitch-switch]]]
            [:label column-name]])
         (:columns data)))])]])

(defn active-filters
  [data table-atom]
  [:div.top__block-filters
   (when-let [filters (utils/block-filter-values @table-atom)]
     [:<>
      [:button.button--light.button.top__clear-filters
       {:on-click #(utils/column-filter-reset-all table-atom)}
       "RESET"]
      (for [[column-key value select] filters]
        (let [normalized-value (-> data :filters :select-normalize column-key)]
          ^{:key (str column-key value)}
          [:button.button.button__active-filters
           {:on-click
            (if select
              #(utils/column-select-filter-reset table-atom column-key (first value))
              #(utils/column-filter-reset table-atom column-key))}
           [:span (cond
                    (and select (not normalized-value)) (second value)
                    select (first value)
                    :else value)]
           [:i.fas.fa-times-circle]]))])])

(defn pagination
  [table-atom processed-rows]
  [:table.table__foot
   [:tfoot
    [:tr
     [:td.foot__pagination
      [:div.pagination__info (utils/pagination-current-and-total-pages processed-rows)]
      [:div.pagination__arrow-group
       [:div.pagination__arrow-nav
        {:class (when (<= @(rf/subscribe [::sub/query-offset]) 0)
                  "pagination__arrow-nav--disabled")
         :on-click #(rf/dispatch [::events/goto-previous-query-page])}
        [:i.fas.fa-chevron-left]]
       [:div.pagination__arrow-nav
        {:class (when (utils/pagination-rows-exhausted? processed-rows)
                  "pagination__arrow-nav--disabled")
         :on-click #(rf/dispatch [::events/goto-next-query-page])}
        [:i.fas.fa-chevron-right]]]]]]])

(defn table
  [data]
  (let [table-atom (r/atom {:utils (dissoc data :columns :rows :filters)})]
    (fn [data]
      (let [{:keys [processed-rows row-count]} (utils/process-rows data @table-atom)]
        [:div.uikit-table
         (if (:loading? data)
           [loading-table {:rows 7 :cols 4}] ;; using previous row count reduces page jumping
           [:div.table__main
            [:table.table
             [header-columns data table-atom]
             [body-rows data table-atom processed-rows]]])
         [pagination table-atom row-count]]))))
