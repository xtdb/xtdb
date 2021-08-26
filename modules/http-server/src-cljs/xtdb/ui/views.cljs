(ns xtdb.ui.views
  (:require [clojure.string :as string]
            [goog.string :refer [format]]
            [cljs.pprint :as pprint]
            [cljs.reader :as reader]
            [xtdb.ui.events :as events]
            [xtdb.ui.codemirror :as cm]
            [xtdb.ui.common :as common]
            [xtdb.ui.subscriptions :as sub]
            [xtdb.ui.uikit.table :as table]
            [xtdb.ui.tab-bar :as tab]
            [xtdb.ui.collapsible :refer [collapsible]]
            [fork.core :as fork]
            [reagent.core :as r]
            [re-frame.core :as rf]
            [tick.alpha.api :as t]
            [cljsjs.react-datetime]))

(def datetime (r/adapt-react-class js/Datetime))

(defn datetime-input [{:keys [values set-values]} key-name]
  [datetime
   {:name key-name
    :dateFormat "YYYY-MM-DD"
    :timeFormat "HH:mm"
    :value (get values key-name)
    :input true
    :on-change (fn [new-value]
                 (try
                   (let [prev-value (get values key-name)]
                     (set-values {key-name (if (.isSame prev-value new-value "day")
                                             new-value
                                             (.startOf new-value "day"))}))
                   (catch js/Error e
                     (set-values {key-name new-value}))))}])

(defn vt-tt-inputs
  [{:keys [values touched errors handle-change handle-blur set-values] :as props} component]
  (let [show-vt? @(rf/subscribe [::sub/show-vt? component])
        show-tt? @(rf/subscribe [::sub/show-tt? component])]
    [:<>
     [:div.crux-time
      [:input {:type :checkbox
               :checked show-vt?
               :on-change #(rf/dispatch [::events/toggle-show-vt component show-vt?])}]
      [:div.input-group-label.label
       [:label "Valid Time"] [:a.label-hint
                              {:href "https://opencrux.com/about/bitemporality.html#valid"
                               :target "_blank"
                               :title "Learn More"}
                              "ⓘ"]]
      [:div.date-time-input
       {:class (when-not show-vt? "hidden")}
       [datetime-input props "valid-time"]
       (when (and (get touched "valid-time")
                  (get errors "valid-time"))
         [:p.input-error (get errors "valid-time")])]]

     [:div.crux-time
      [:input {:type :checkbox
               :checked show-tt?
               :on-change #(rf/dispatch [::events/toggle-show-tt component show-tt?])}]
      [:div.input-group-label.label
       [:label "Transaction Time" ] [:a.label-hint
                                     {:href "https://opencrux.com/about/bitemporality.html#transaction"
                                      :target "_blank"
                                      :title "Learn More"}
                                     "ⓘ"]]
      [:div.date-time-input
       {:class (when-not show-tt? "hidden")}
       [datetime-input props "tx-time"]
       (when (and (get touched "tx-time")
                  (get errors "tx-time"))
         [:p.input-error (get errors "tx-time")])]]]))

(defn vt-tt-entity-box
  [vt tt]
  [:div.entity-vt-tt
   [:div.entity-vt-tt__title
    "Valid Time"]
   [:div.entity-vt-tt__value (str vt)]
   [:div.entity-vt-tt__title
    "Transaction Time"]
   [:div.entity-vt-tt__value (str tt)]])

(defn remove-times-if-hidden
  [form-values component]
  (cond-> form-values
    (not @(rf/subscribe [::sub/show-vt? component])) (assoc-in [:values "valid-time"] nil)
    (not @(rf/subscribe [::sub/show-tt? component])) (assoc-in [:values "tx-time"] nil)))

(defn query-validation
  [values]
  (let [invalid-query? (try
                         (let [query-edn (reader/read-string (get values "q"))]
                           (cond
                             (nil? query-edn)  "Query box is empty"
                             (not (contains? query-edn :find)) "Query doesn't contain a 'find' clause"
                             (not (contains? query-edn :where)) "Query doesn't contain a 'where' clause"))
                         (catch js/Error e
                           (str "Error reading query - " (.-message e))))
        invalid-time? (fn [key-name] (try
                                       (let [result (js/moment (get values key-name))]
                                         (when (string/includes? (str result) "Invalid")
                                           (str result)))
                                       (catch js/Error e
                                         (str "Error reading time - " (.-message e)))))
        validation {"q" invalid-query?
                    "valid-time" (when @(rf/subscribe [::sub/show-vt? :query])
                                   (invalid-time? "valid-time"))
                    "tx-time" (when @(rf/subscribe [::sub/show-tt? :query])
                                      (invalid-time? "tx-time"))}]
    (when (some some? (vals validation)) validation)))

(defn- submit-form-on-keypress [evt form-id]
  (when (and (.-ctrlKey evt) (= 13 (.-keyCode evt)))
    (let [form-button (js/document.querySelector (str form-id " button"))]
      (.click form-button))))

(defn edit-query [_ props]
  ;; we need to create a cm instance holder to modify the CodeMirror code
  (let [cm-instance (atom nil)]
    (fn [{:keys [values errors touched set-values set-touched] :as props}]
      [:<>
       [:div.input-textarea
        [cm/code-mirror (get values "q")
         {:cm-instance cm-instance
          :class "cm-textarea__query"
          :on-change #(set-values {"q" %})
          :on-blur #(set-touched "q")}]]
       (when (and (get touched "q")
                  (get errors "q"))
         [:p.input-error (get errors "q")])
       [vt-tt-inputs props :query]])))

(defn query-history
  [set-values]
  (let [query-history-list @(rf/subscribe [::sub/query-form-history])]
    [:<>
     (if (not-empty query-history-list)
       [:div.form-pane__history-scrollable
        (reverse
         (map-indexed
          (fn [idx {:strs [q] :as history-q}]
            ^{:key (gensym)}
            [:div.form-pane__history-scrollable-el
             [:div.form-pane__history-scrollable-el-left
              {:on-click #(do (set-values {"q" q})
                              (rf/dispatch [::events/query-form-tab-selected :edit-query]))}
              [:div
               [cm/code-mirror-static q {:class "cm-textarea__query"}]]]
             [:div.form-pane__history-delete
              {:on-click #(rf/dispatch [::events/remove-query-from-local-storage idx])}
              [:i.fas.fa-trash-alt]]])
          query-history-list))]
       [:div.form-pane__history-empty
        [:b "No recent queries found."]])]))

(defn query-form
  []
  (let [form-id "#form-query"
        event-listener #(submit-form-on-keypress % form-id)]
    (r/create-class
     {:component-did-mount (fn []
                             (-> js/window
                                 (.addEventListener "keydown" event-listener)))
      :component-will-unmount (fn []
                                (-> js/window
                                    (.removeEventListener "keydown" event-listener)))
      :reagent-render
      (fn []
        [fork/form {:form-id (subs form-id 1)
                    :validation query-validation
                    :prevent-default? true
                    :clean-on-unmount? true
                    :initial-values @(rf/subscribe [::sub/initial-values-query])
                    :on-submit #(rf/dispatch [::events/go-to-query-view (remove-times-if-hidden % :query)])}
         (fn [{:keys [values errors touched set-values set-touched form-id handle-submit] :as props}]
           (let [loading? @(rf/subscribe [::sub/query-result-pane-loading?])
                 disabled? (or loading? (some some? (vals errors)))]
             [:form {:id form-id, :on-submit handle-submit}
              [collapsible [::query-form ::query-editor] {:label "Datalog Query Editor" :default-open? true}
               [tab/tab-bar {:tabs [{:k :edit-query, :label "Edit Query"}
                                    {:k :recent-queries, :label "Recent Queries"}]
                             :current-tab [::sub/query-form-tab]
                             :on-tab-selected [::events/query-form-tab-selected]}]

               (case @(rf/subscribe [::sub/query-form-tab])
                 :edit-query [edit-query props]
                 :recent-queries [query-history set-values])]

              [:p
               [:button.button
                {:type "submit"
                 :class (when-not disabled? "form__button")
                 :disabled disabled?}
                "Run Query"]]]))])})))

(defn query-table
  []
  (let [{:keys [error data]} @(rf/subscribe [::sub/query-data-table])]
    [:<>
     (cond
       error [:div.error-box (str error)]
       (and (empty? (:rows data)) (not (:loading? data))) [:div.no-results "No results found!"]
       :else [:<>
              [:<>
               [:div.query-table-topbar
                (let [limit @(rf/subscribe [::sub/query-limit])
                      [start-time end-time] @(rf/subscribe [::sub/request-times])
                      row-count (count (:rows data))
                      count-string (if (> row-count limit) (str limit "+") (str row-count))]
                  [:div.query-result-info
                   [:b count-string] " result" (when (> row-count 1) "s")
                   (when (not= start-time end-time)
                     [:<>
                      " in " [:b (common/format-duration->seconds (t/between start-time end-time))] " seconds"])])
                [:div.query-table-downloads
                 [:a.query-table-downloads__link
                  {:href @(rf/subscribe [::sub/query-data-download-link "csv"])}
                  "CSV"]
                 "|"
                 [:a.query-table-downloads__link
                  {:href @(rf/subscribe [::sub/query-data-download-link "tsv"])}
                  "TSV"]]]]
              [table/table data]])]))

(defn query-pane
  []
  [:<>
   [query-form]
   (when @(rf/subscribe [::sub/query-submitted?])
     [query-table])])

(defn entity-validation
  [values]
  (let [empty-string? #(empty? (string/trim (or (get values %) "")))
        invalid-time? (fn [key-name] (try
                                       (let [result (js/moment (get values key-name))]
                                         (when (string/includes? (str result) "Invalid")
                                           (str result)))
                                       (catch js/Error e
                                         (str "Error reading time - " (.-message e)))))
        validation {"eid" (when (empty-string? "eid")
                            "Entity id is empty")
                    "valid-time" (when @(rf/subscribe [::sub/show-vt? :entity])
                                   (invalid-time? "valid-time"))
                    "tx-time" (when @(rf/subscribe [::sub/show-tt? :entity])
                                      (invalid-time? "tx-time"))}]
    (when (some some? (vals validation)) validation)))

(defn entity-form
  []
  (let [form-id "#form-entity"
        event-listener #(submit-form-on-keypress % form-id)]
    (r/create-class
     {:component-did-mount (fn []
                             (-> js/window
                                 (.addEventListener "keydown" event-listener)))
      :component-will-unmount (fn []
                                (-> js/window
                                    (.removeEventListener "keydown" event-listener)))
      :reagent-render
      (fn []
        [fork/form {:form-id (subs form-id 1)
                    :prevent-default? true
                    :clean-on-unmount? true
                    :validation entity-validation
                    :initial-values @(rf/subscribe [::sub/initial-values-entity])
                    :on-submit #(rf/dispatch [::events/go-to-entity-view (remove-times-if-hidden % :entity)])}
         (fn [{:keys [values
                      touched
                      errors
                      form-id
                      set-values
                      set-touched
                      handle-change
                      handle-blur
                      handle-submit] :as props}]
           (let [loading? @(rf/subscribe [::sub/entity-result-pane-loading?])
                 disabled? (or loading? (some some? (vals errors)))]
             [:form
              {:id form-id
               :on-submit handle-submit}
              [:div.entity-form__input-line
               [:span {:style {:padding-right "1rem"}}
                "Entity "
                [:b ":xt/id"]]
               [:input.monospace.entity-form__input
                {:type "text"
                 :name "eid"
                 :value (get values "eid")
                 :placeholder "e.g. :my-ns/example-id or #uuid \"...\""
                 :on-change handle-change
                 :on-blur handle-blur}]]
              [vt-tt-inputs props :entity]
              [:div.button-line
               [:button.button
                {:type "submit"
                 :class (when-not disabled? "form__button")
                 :disabled disabled?}
                "Fetch"]]]))])})))

(defn entity-document
  []
  (let [!raw-edn? (r/atom false)]
    (fn []
      (let [{:keys [eid vt tt document linked-entities error]}
            @(rf/subscribe [::sub/entity-result-pane-document])
            loading? @(rf/subscribe [::sub/entity-result-pane-loading?])]
        [:<>
         [:div.history-diffs__options
          [:div.history-checkbox__group
           [:div.history-diffs__checkbox
            [:input
             {:checked @!raw-edn?
              :on-change #(swap! !raw-edn? not)
              :type "checkbox"}]]
           [:span "Raw EDN"]]]
         (if loading?
           [:div.entity-map.entity-map--loading
            [:i.fas.fa-spinner.entity-map__load-icon]]
           (if error
             [:div.error-box (str error)]
             [:div.entity-map__container
              (if @!raw-edn?
                [:div.entity-raw-edn
                 (with-out-str (pprint/pprint document))]
                [::div.entity-map
                 [cm/code-snippet document linked-entities]])
              [vt-tt-entity-box vt tt]]))]))))

(defn- entity-history-document []
  (let [!diffs? (r/atom false)]
    (fn []
      (let [diffs? @!diffs?
            entity-error @(rf/subscribe [::sub/entity-result-pane-document-error])
            loading? @(rf/subscribe [::sub/entity-result-pane-loading?])
            {:keys [query-params path-params]} @(rf/subscribe [::sub/current-route])
            asc-order? (= "asc" (:sort-order query-params))]
        [:<>
         [:div.history-diffs__options
          [:div.select.history-sorting-group
           [:select
            {:name "diffs-order"
             :value (:sort-order query-params)
             :on-change #(rf/dispatch [:navigate :entity path-params
                                       (assoc query-params :sort-order (if asc-order? "desc" "asc"))])}
            [:option {:value "asc"} "Ascending"]
            [:option {:value "desc"} "Descending"]]]
          [:div.history-checkbox__group
           [:div.history-diffs__checkbox
            [:input
             {:checked diffs?
              :on-change #(swap! !diffs? not)
              :type "checkbox"}]]
           [:span "Diffs"]]]
         [:div.entity-histories__container
          (if loading?
            [:div.entity-map.entity-map--loading
             [:i.fas.fa-spinner.entity-map__load-icon]]
            (cond
              entity-error [:div.error-box (str entity-error)]
              (not diffs?) (let [{:keys [entity-history]} @(rf/subscribe [::sub/entity-result-pane-history])]
                             [:div.entity-histories
                              (for [{:keys [xt/tx-time xt/valid-time xt/doc]
                                     :as history-elem} entity-history]
                                ^{:key history-elem}
                                [:div.entity-history__container
                                 [:div.entity-map
                                  [cm/code-snippet doc {}]]
                                 [vt-tt-entity-box valid-time tx-time]])])
              diffs? (let [{:keys [up-to-date-doc history-diffs]} @(rf/subscribe [::sub/entity-result-pane-history-diffs])]
                       [:div.entity-histories
                        [:div.entity-history__container
                         [:div.entity-map
                          [cm/code-snippet (:xt/doc up-to-date-doc) {}]]
                         [vt-tt-entity-box
                          (:xt/valid-time up-to-date-doc)
                          (:xt/tx-time up-to-date-doc)]]
                        (for [{:keys [additions deletions
                                      xt/tx-time xt/valid-time]
                               :as history-elem} history-diffs]
                          ^{:key history-elem}
                          [:div.entity-history__container
                           [:div.entity-map__diffs-group
                            [:div.entity-map
                             (when additions
                               [:<>
                                [:span {:style {:color "green"}}
                                 "+ Additions:"]
                                [cm/code-snippet additions {}]])
                             (when deletions
                               [:<>
                                [:span {:style {:color "red"}}
                                 "- Deletions:"]
                                [cm/code-snippet deletions {}]])]]
                           [vt-tt-entity-box valid-time tx-time]])])
              :else nil))]]))))

(defn entity-pane []
  [:<>
   [entity-form]

   (when @(rf/subscribe [::sub/eid-submitted?])
     [:<>
      [tab/tab-bar {:tabs [{:k :document, :label "Document", :dispatch [::events/set-entity-pane-document]}
                           {:k :history, :label "History", :dispatch [::events/set-entity-pane-history]}]
                    :current-tab [::sub/entity-pane-tab]
                    :on-tab-selected [::events/entity-pane-tab-selected]}]

      (case @(rf/subscribe [::sub/entity-pane-tab])
        :document [entity-document]
        :history [entity-history-document])])])

(defn render-status-map
  [status-map]
  [:div.node-info__content
   (for [[key value] (common/sort-map status-map)]
     (when value
       ^{:key key}
       [:p
        [:span.node-info__key (common/edn->pretty-string key)]
        [:span.node-info__value (common/edn->pretty-string value)]]))])

(defn render-attribute-stats
  [attributes-map]
  [:div.node-info__content
   [:table.table
    [:thead.table__head
     [:tr
      [:th "Attribute"]
      [:th "Count (across all versions)"]]]
    [:tbody.table__body
     [:<>
      (for [[key value] (sort-by (juxt val key) #(compare %2 %1) attributes-map)]
        ^{:key key}
        [:tr.table__row.body__row
         [:td.table__cell.body__cell
          [:a
           {:href (common/route->url :query nil {:query-edn {:find [(symbol (name key))]
                                                             :where [['e key (symbol (name key))]]}})}
           (common/edn->pretty-string key)]]
         [:td.table__cell.body__cell (common/edn->pretty-string value)]])]]]])

(defn render-metrics-map
  [metrics-map]
  (if metrics-map
    [render-status-map metrics-map]
    [:div.node-info__content
     [:span.metrics-warning
      "You do not have the metrics status reporter enabled - to read about metrics in Crux, see "
      [:a {:href "https://opencrux.com/reference/monitoring.html"} "here"]
      "."]]))

(defn status-pane
  []
  (let [status-map @(rf/subscribe [::sub/node-status])
        options-map @(rf/subscribe [::sub/node-options])
        attributes-map @(rf/subscribe [::sub/node-attribute-stats])]
    [:<>
     [tab/tab-bar {:tabs [{:k :overview, :label "Overview"}
                          {:k :configuration, :label "Current Configuration"}
                          {:k :attributes, :label "Attribute Cardinalities"}
                          {:k :metrics, :label "Node Metrics"}]
                   :current-tab [::sub/status-tab]
                   :on-tab-selected [::events/status-tab-selected]}]

     (case @(rf/subscribe [::sub/status-tab])
       :overview [render-status-map (dissoc status-map :xtdb.metrics)]
       :configuration [render-status-map options-map]
       :attributes [render-attribute-stats attributes-map]
       :metrics (render-metrics-map (:xtdb.metrics status-map)))]))

(defn console-pane []
  [:<>
   [tab/tab-bar {:tabs [{:k :query, :label "Datalog Query"}
                        {:k :entity, :label "Browse Documents"}
                        {:k :status, :label "Node Status"}]
                 :current-tab [::sub/console-tab]
                 :on-tab-selected [::events/console-tab-selected]}]
   (case @(rf/subscribe [::sub/console-tab])
     :query [query-pane]
     :entity [entity-pane]
     :status [status-pane])])

(defn view []
  (let [{{:keys [name]} :data} @(rf/subscribe [::sub/current-route])]
    [:div.container.page-pane
     [console-pane]]))
