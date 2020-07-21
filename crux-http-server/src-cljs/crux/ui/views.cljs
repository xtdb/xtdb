(ns crux.ui.views
  (:require [clojure.string :as string]
            [cljs.pprint :as pprint]
            [cljs.reader :as reader]
            [crux.ui.events :as events]
            [crux.ui.codemirror :as cm]
            [crux.ui.common :as common]
            [crux.ui.subscriptions :as sub]
            [crux.ui.uikit.table :as table]
            [crux.ui.tab-bar :as tab]
            [crux.ui.collapsible :refer [collapsible]]
            [fork.core :as fork]
            [reagent.core :as r]
            [re-frame.core :as rf]
            [tick.alpha.api :as t]))

(defn vt-tt-inputs
  [{:keys [values touched errors handle-change handle-blur]} component]
  (let [show-vt? @(rf/subscribe [::sub/show-vt? component])
        show-tt? @(rf/subscribe [::sub/show-tt? component])]
    [:<>
     [:div.crux-time
      [:input {:type :checkbox
               :checked show-vt?
               :on-change #(rf/dispatch [::events/toggle-show-vt component show-vt?])}]
      [:div.input-group-label.label
       [:label "Valid Time"]]

      [:div
       {:class (when-not show-vt? "hidden")}
       [:input.input.input-time
        {:type "date"
         :name "vtd"
         :value (get values "vtd")
         :on-change handle-change
         :on-blur handle-blur}]
       [:input.input.input-time
        {:type "time"
         :name "vtt"
         :value (get values "vtt")
         :on-change handle-change
         :on-blur handle-blur}]
       (when (and (or (get touched "vtd")
                      (get touched "vtt"))
                  (get errors "vt"))
         [:p.input-error (get errors "vt")])]]

     [:div.crux-time
      [:input {:type :checkbox
               :checked show-tt?
               :on-change #(rf/dispatch [::events/toggle-show-tt component show-tt?])}]
      [:div.input-group-label.label
       [:label "Transaction Time" ]]
      [:div
       {:class (when-not show-tt? "hidden")}
       [:input.input.input-time
        {:type "date"
         :name "ttd"
         :value (get values "ttd")
         :on-change handle-change
         :on-blur handle-blur}]
       [:input.input.input-time
        {:type "time"
         :name "ttt"
         :value (get values "ttt")
         :on-change handle-change
         :on-blur handle-blur}]
       (when (and (or (get touched "ttd")
                      (get touched "ttt"))
                  (get errors "tt"))
         [:p.input-error (get errors "tt")])]]]))

(defn vt-tt-entity-box
  [vt tt]
  [:div.entity-vt-tt
   [:div.entity-vt-tt__title
    "Valid Time"]
   [:div.entity-vt-tt__value (str vt)]
   [:div.entity-vt-tt__title
    "Transaction Time"]
   [:div.entity-vt-tt__value (str tt)]])

(defn query-validation
  [values]
  (let [empty-string? #(empty? (string/trim (or (get values %) "")))
        invalid-query? (try
                         (let [query-edn (reader/read-string (get values "q"))]
                           (cond
                             (nil? query-edn)  "Query box is empty"
                             (not (contains? query-edn :find)) "Query doesn't contain a 'find' clause"
                             (not (contains? query-edn :where)) "Query doesn't contain a 'where' clause"))
                         (catch js/Error e
                           (str "Error reading query - " (.-message e))))
        validation {"q" invalid-query?
                    "vt" (when (apply not= ((juxt #(% "vtd")
                                                  #(% "vtt")) empty-string?))
                           "Fill out both inputs or none")
                    "tt" (when (apply not= ((juxt #(% "ttd")
                                                  #(% "ttt")) empty-string?))
                           "Fill out both inputs or none")}]
    (when (some some? (vals validation)) validation)))

(defn- submit-form-on-keypress [evt form-id]
  (when (and (.-ctrlKey evt) (= 13 (.-keyCode evt)))
    (let [form-button (js/document.querySelector (str form-id " button"))]
      (.click form-button))))

(defn edit-query [_ props]
  ;; we need to create a cm instance holder to modify the CodeMirror code
  (let [cm-instance (atom nil)]
    (fn [{:keys [values errors touched set-values set-touched form-id handle-submit] :as props}]
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
  []
  (let [query-history-list @(rf/subscribe [::sub/query-form-history])]
    [:div.form-pane__history-scrollable
     (map-indexed
      (fn [idx {:strs [q valid-time transaction-time] :as history-q}]
        ^{:key (gensym)}
        [:div.form-pane__history-scrollable-el
         [:div.form-pane__history-delete
          {:on-click #(rf/dispatch [::events/remove-query-from-local-storage idx])}
          [:i.fas.fa-trash-alt]]
         [:div.form-pane__history-scrollable-el-left
          {:on-click #(rf/dispatch [::events/go-to-historical-query history-q])}
          (when valid-time
            [:div
             {:style {:margin-bottom "1rem"}}
             [:span.form-pane__history-headings "Valid Time: "]
             [:span.form-pane__history-txt valid-time]])
          (when transaction-time
            [:div
             [:span.form-pane__history-headings "Transaction Time: "]
             [:span.form-pane__history-txt transaction-time]])
          [:div {:style {:margin-top "1rem"}}
           [cm/code-mirror-static q {:class "cm-textarea__query"}]]]])
      query-history-list)]))

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
                    :on-submit #(rf/dispatch [::events/go-to-query-view %])}
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
                 :recent-queries [query-history])]

              [:p
               [:button.button
                {:type "submit"
                 :class (when-not disabled? "form__button")
                 :disabled disabled?}
                "Run Query"]]]))])})))

(defn query-table
  []
  (let [{:keys [error data]} @(rf/subscribe [::sub/query-data-table])
        limit @(rf/subscribe [::sub/query-limit])
        loading? @(rf/subscribe [::sub/query-result-pane-loading?])
        [start-time end-time] @(rf/subscribe [::sub/request-times])]
    [:<>
     (cond
       error [:div.error-box error]
       loading? ""
       (and
        (:rows data)
        (empty? (:rows data))) [:div.no-results "No results found!"]
       :else [:<>
              [:<>
               (let [c (min limit (count (:rows data)))] [:div "Found " [:b c] " result tuple" (if (> c 1) "s" "") ""])
               (if (= start-time end-time)
                 ""
                 [:div "Round-trip time: "
                  [:b
                   (common/format-duration->seconds (t/between start-time end-time))] " seconds"])]
              [:div.query-table-downloads
               "Download results as:"
               [:a.query-table-downloads__link
                {:href @(rf/subscribe [::sub/query-data-download-link "csv"])}
                "CSV"]
               [:a.query-table-downloads__link
                {:href @(rf/subscribe [::sub/query-data-download-link "tsv"])}
                "TSV"]]
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
        validation {"eid" (when (empty-string? "eid")
                            "Entity id is empty")
                    "vt" (when (apply not= ((juxt #(% "vtd")
                                                  #(% "vtt")) empty-string?))
                           "Fill out both inputs or none")
                    "tt" (when (apply not= ((juxt #(% "ttd")
                                                  #(% "ttt")) empty-string?))
                           "Fill out both inputs or none")}]
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
                    :on-submit #(rf/dispatch [::events/go-to-entity-view %])}
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
                [:b ":crux.db/id"]]
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
             [:div.error-box error]
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
              entity-error [:div.error-box entity-error]
              (not diffs?) (let [{:keys [entity-history]} @(rf/subscribe [::sub/entity-result-pane-history])]
                             [:div.entity-histories
                              (for [{:keys [crux.tx/tx-time crux.db/valid-time crux.db/doc]
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
                          [cm/code-snippet (:crux.db/doc up-to-date-doc) {}]]
                         [vt-tt-entity-box
                          (:crux.db/valid-time up-to-date-doc)
                          (:crux.tx/tx-time up-to-date-doc)]]
                        (for [{:keys [additions deletions
                                      crux.tx/tx-time crux.db/valid-time]
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

(defn console-pane []
  [:<>
   [tab/tab-bar {:tabs [{:k :query, :label "Datalog Query"}
                        {:k :entity, :label "Browse Documents"}]
                 :current-tab [::sub/console-tab]
                 :on-tab-selected [::events/console-tab-selected]}]
   (case @(rf/subscribe [::sub/console-tab])
     :query [query-pane]
     :entity [entity-pane])])

(defn status-map->html-elements [status-map]
  (into
   [:dl.node-info__content]
   (mapcat
    (fn [[key value]]
      (when value
        [[:dt [:b (str key)]]
         (cond
           (map? value) [:dd (into
                              [:dl]
                              (mapcat
                               (fn [[key value]]
                                 [[:dt [:b (str key)]]
                                  [:dd (with-out-str (pprint/pprint value))]])
                               value))]
           :else [:dd (with-out-str (pprint/pprint value))])]))
    (common/sort-map status-map))))

(defn status-page
  []
  (let [status-map @(rf/subscribe [::sub/node-status])
        options-map @(rf/subscribe [::sub/node-options])
        attributes-map @(rf/subscribe [::sub/node-attribute-stats])
        loading? (or @(rf/subscribe [::sub/node-status-loading?])
                     @(rf/subscribe [::sub/node-attribute-stats-loading?]))]
    (when (and (some? loading?) (not loading?))
      [:div.node-info__container
       [:div.node-info
        [:h2.node-info__title "Node Status"]
        [status-map->html-elements status-map]]
       [:div.node-info
        [:h2.node-info__title "Node Options"]
        [status-map->html-elements options-map]]
       [:div.node-info
        [:h2.node-info__title "Attribute Cardinalities"]
        [status-map->html-elements attributes-map]]
       ])))

(defn root-page
  []
  [:div.root-page
   [:div.root-background]
   [:div.root-contents
    [:h1.root-title "Console Overview"]
;;    [:h2.root-subtitle "Small Description Here"]
    [:div.root-info-summary
     [:div.root-info "✓ Crux node is active"]
     [:div.root-info "✓ HTTP is enabled"]]
    [:div.root-tiles
     [:a.root-tile
      {:href (common/route->url :query)}
      [:i.fas.fa-search]
      [:br]
      "Query"]
     [:a.root-tile
      {:href (common/route->url :status)}
      [:i.fas.fa-wrench]
      [:br]
      "Status"]
     [:a.root-tile
      {:href "https://opencrux.com/docs" :target "_blank"}
      [:i.fas.fa-book]
      [:br]
      "Docs"]]]])

(defn view []
  (let [{{:keys [name]} :data} @(rf/subscribe [::sub/current-route])]
    [:div.container.page-pane
     (cond
       (= name :homepage) [root-page]
       (= name :status) [status-page]
       :else [console-pane])]))
