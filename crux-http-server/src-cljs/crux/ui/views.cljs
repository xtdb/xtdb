(ns crux.ui.views
  (:require
   [clojure.string :as string]
   [cljs.pprint :as pprint]
   [cljs.reader :as reader]
   [crux.ui.events :as events]
   [crux.ui.codemirror :as cm]
   [crux.ui.common :as common]
   [crux.ui.subscriptions :as sub]
   [crux.ui.uikit.table :as table]
   [fork.core :as fork]
   [reagent.core :as r]
   [re-frame.core :as rf]))

(defn vt-tt-display
  [component]
  (let [show-vt? @(rf/subscribe [::sub/show-vt? component])
        show-tt? @(rf/subscribe [::sub/show-tt? component])]
    [:<>
     [:div.expand-collapse__group
      {:on-click #(rf/dispatch [::events/toggle-show-vt component show-vt?])}
      [:span.expand-collapse__txt
       [:span.form-pane__arrow
        [common/arrow-svg show-vt?] "Valid Time"]]]
     [:div.expand-collapse__group
      {:on-click #(rf/dispatch [::events/toggle-show-tt component show-tt?])}
      [:span.expand-collapse__txt
       [:span.form-pane__arrow
        [common/arrow-svg show-tt?] "Transaction Time"]]]]))

(defn vt-tt-inputs
  [{:keys [values touched errors handle-change handle-blur]} component]
  (let [show-vt? @(rf/subscribe [::sub/show-vt? component])
        show-tt? @(rf/subscribe [::sub/show-tt? component])]
    [:div.crux-time
     [:div.input-group
      {:class (when-not show-vt? "hidden")}
      [:div.input-group-label.label
       [:label "Valid Time"]]
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
        [:p.input-error (get errors "vt")])]
     [:div.input-group
      {:class (when-not show-tt? "hidden")}
      [:div.input-group-label.label
       [:label "Transaction Time" ]]
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
        [:p.input-error (get errors "tt")])]]))

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

(defn query-form
  []
  ;; we need to create a cm instance holder to modify the CodeMirror code
  (let [cm-instance (atom nil)
        form-id "#form-query"
        query-root? (= :query-root @(rf/subscribe [::sub/query-pane-view]))]
    (r/create-class {:component-did-mount (fn []
                                            (-> (js/document.querySelector form-id)
                                                (.addEventListener "keydown" #(submit-form-on-keypress % form-id) true)))
                     :component-will-unmount (fn []
                                               (-> (js/document.querySelector form-id)
                                                   (.removeEventListener "keydown" #(submit-form-on-keypress % form-id) true)))
                     :reagent-render
                     (fn []
                       [fork/form {:form-id (subs form-id 1)
                                   :validation query-validation
                                   :prevent-default? true
                                   :clean-on-unmount? true
                                   :initial-values @(rf/subscribe [::sub/initial-values-query])
                                   :on-submit #(rf/dispatch [::events/go-to-query-view %])}
                        (fn [{:keys [values
                                     errors
                                     touched
                                     set-values
                                     set-touched
                                     form-id
                                     handle-submit] :as props}]
                          (let [loading? @(rf/subscribe [::sub/query-result-pane-loading?])
                                disabled? (or loading? (some some? (vals errors)))]
                            [:<>
                             [:form
                              {:id form-id
                               :on-submit handle-submit}
                              [:div.input-textarea
                               [cm/code-mirror (get values "q")
                                {:cm-instance cm-instance
                                 :class "cm-textarea__query"
                                 :on-change #(set-values {"q" %})
                                 :on-blur #(set-touched "q")}]]
                              [:div.query-form-options
                               [vt-tt-display :query]]
                              (when (and (get touched "q")
                                         (get errors "q"))
                                [:p.input-error (get errors "q")])
                              [vt-tt-inputs props :query]
                              [:div.button-line
                               [:button.button
                                {:type "submit"
                                 :class (when-not disabled? "form__button")
                                 :disabled disabled?}
                                "Submit Query"]
                               [:p.button-hint "(Ctrl + Enter)"]]]]))])})))

(defn entity-validation
  [values]
  (let [empty-string? #(empty? (string/trim (or (get values %) "")))
        validation {"eid" (when (empty? (common/strip-commented-lines (get values "eid")))
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
  (let [form-id "#form-entity"]
    (r/create-class
     {:component-did-mount (fn []
                             (-> (js/document.querySelector form-id)
                                 (.addEventListener "keydown" #(submit-form-on-keypress % form-id) true)))
      :component-will-unmount (fn []
                                (-> (js/document.querySelector form-id)
                                    (.removeEventListener "keydown" #(submit-form-on-keypress % form-id) true)))
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
                      handle-submit] :as props}]
           (let [loading? @(rf/subscribe [::sub/entity-result-pane-loading?])
                 disabled? (or loading? (some some? (vals errors)))]
             [:form
              {:id form-id
               :on-submit handle-submit}
              [:div.input-textarea
               [cm/code-mirror (get values "eid")
                {:class "cm-textarea__entity"
                 :on-change #(set-values {"eid" %})
                 :on-blur #(set-touched "eid")}]
               (when (and (get touched "eid")
                          (get errors "eid"))
                 [:p.input-error (get errors "eid")])]
              [:div.query-form-options
               [vt-tt-display :entity]]
              [vt-tt-inputs props :entity]
              [:div.button-line
               [:button.button
                {:type "submit"
                 :class (when-not disabled? "form__button")
                 :disabled disabled?}
                "Submit Entity"]
               [:p.button-hint "(Ctrl + Enter)"]]]))])})))

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

(defn query-table
  []
  (let [{:keys [error data]} @(rf/subscribe [::sub/query-data-table])
        loading? @(rf/subscribe [::sub/query-result-pane-loading?])]
    [:<>
     (cond
       error [:div.error-box error]
       (and
        (:rows data)
        (empty? (:rows data))) [:div.no-results "No results found!"]
       :else [:<>
              [table/table data]
              [:div.query-table-downloads
               "Download as:"
               [:a.query-table-downloads__link
                {:href @(rf/subscribe [::sub/query-data-download-link "csv"])}
                "CSV"]
               [:a.query-table-downloads__link
                {:href @(rf/subscribe [::sub/query-data-download-link "tsv"])}
                "TSV"]]])]))

(defn vt-tt-entity-box
  [vt tt]
  [:div.entity-vt-tt
   [:div.entity-vt-tt__title
    "Valid Time"]
   [:div.entity-vt-tt__value (str vt)]
   [:div.entity-vt-tt__title
    "Transaction Time"]
   [:div.entity-vt-tt__value (str tt)]])

(defn entity-document
  []
  (let [{:keys [eid vt tt document linked-entities error]}
        @(rf/subscribe [::sub/entity-result-pane-document])
        loading? @(rf/subscribe [::sub/entity-result-pane-loading?])]
    [:div.entity-map__container
     (if loading?
       [:div.entity-map.entity-map--loading
        [:i.fas.fa-spinner.entity-map__load-icon]]
       (if error
         [:div.error-box error]
         [:<>
          [::div.entity-map
           [cm/code-snippet document linked-entities]]
          [vt-tt-entity-box vt tt]]))]))

(defn- entity-history-document []
  (let [diffs-tab? @(rf/subscribe [::sub/entity-result-pane-history-diffs?])
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
         {:checked diffs-tab?
          :on-change #(rf/dispatch [::events/set-entity-result-pane-history-diffs?
                                    (if diffs-tab? false true)])
          :type "checkbox"}]]
       [:span "Diffs"]]]
     [:div.entity-histories__container
      (if loading?
        [:div.entity-map.entity-map--loading
         [:i.fas.fa-spinner.entity-map__load-icon]]
        (cond
          entity-error [:div.error-box entity-error]
          (not diffs-tab?) (let [{:keys [entity-history]} @(rf/subscribe [::sub/entity-result-pane-history])]
                             [:div.entity-histories
                              (for [{:keys [crux.tx/tx-time crux.db/valid-time crux.db/doc]
                                     :as history-elem} entity-history]
                                ^{:key history-elem}
                                [:div.entity-history__container
                                 [:div.entity-map
                                  [cm/code-snippet doc {}]]
                                 [vt-tt-entity-box valid-time tx-time]])])
          diffs-tab? (let [{:keys [up-to-date-doc history-diffs]} @(rf/subscribe [::sub/entity-result-pane-history-diffs])]
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
          :else nil))]]))

(defn entity-raw-edn
  []
  (let [{:keys [document error]}
        @(rf/subscribe [::sub/entity-result-pane-document])
        loading? @(rf/subscribe [::sub/entity-result-pane-loading?])]
    [:div.entity-raw-edn__container
     (if loading?
       [:div.entity-map.entity-map--loading
        [:i.fas.fa-spinner.entity-map__load-icon]]
       (if error
         [:div.error-box error]
         [:div.entity-raw-edn
          (with-out-str (pprint/pprint document))]))]))


(defn entity-pane []
  (let [pane-view @(rf/subscribe [::sub/entity-pane-view])]
    (when-not (= pane-view :entity-root)
      [:<>
       [:div.pane-nav
        [:div.pane-nav__tab
         {:class (when (= pane-view :document) "pane-nav__tab--active")
          :on-click #(rf/dispatch [::events/set-entity-pane-document])}
         "Document"]
        [:div.pane-nav__tab
         {:class (when (= pane-view :history) "pane-nav__tab--active")
          :on-click #(rf/dispatch [::events/set-entity-pane-history])}
         "History"]
        [:div.pane-nav__tab
         {:class (if (= pane-view :raw-edn)
                   "pane-nav__tab--active"
                   "pane-nav__tab--hover")
          :on-click #(rf/dispatch [::events/set-entity-pane-raw-edn])}
         "Raw EDN"]]
       [:section.document-section
        (case pane-view
          :document [entity-document]
          :history [entity-history-document]
          :raw-edn [entity-raw-edn]
          nil)]])))

(defn query-pane
  []
  (let [pane-view @(rf/subscribe [::sub/query-pane-view])]
    (when-not (= pane-view :query-root)
      [query-table])))

(defn form-pane
  []
  (let [form-pane-hidden? @(rf/subscribe [::sub/form-pane-hidden?])
        form-pane-view @(rf/subscribe [::sub/form-pane-view])]
    [:div.form-pane
     [:div.expand-collapse__group
      {:on-click #(rf/dispatch [::events/toggle-form-pane])}
      [:span.expand-collapse__txt
       [:span.form-pane__arrow
        [common/arrow-svg (not form-pane-hidden?)] "Console"]]]

     [:div
      {:class (if form-pane-hidden? "collapse" "expand")}
      [:div.form-pane__tabs
       [:div.form-pane__tab
        {:class (when (= :query form-pane-view)
                  "form-pane__tab--selected")
         :on-click #(rf/dispatch [::events/set-form-pane-view :query])}
        "Query"]
       [:div.form-pane__tab
        {:class (when (= :entity form-pane-view)
                  "form-pane__tab--selected")
         :on-click #(rf/dispatch [::events/set-form-pane-view :entity])}
        "Entity"]
       [:div.form-pane__tab
        {:class (when (= :query-history form-pane-view)
                  "form-pane__tab--selected")
         :on-click #(rf/dispatch [::events/set-form-pane-view :query-history])}
        "Query History"]]
      [:div.form-pane__content
       [:div
        {:class (if (= :query form-pane-view) "visible" "hidden")}
        [query-form]]
       [:div
        {:class (if (= :entity form-pane-view) "visible" "hidden")}
        [entity-form]]
       [:div
        {:class (if (= :query-history form-pane-view) "visible" "hidden")}
        [query-history]]]]]))

(defn status-map->html-elements [status-map]
  (into
   [:div.node-info__content]
   (map
    (fn [[key value]]
      (when value
        [:p [:b (str key)] ": " (str value)]))
    status-map)))

(defn status-page
  []
  (let [status-map @(rf/subscribe [::sub/node-status])
        options-map @(rf/subscribe [::sub/node-options])
        loading? @(rf/subscribe [::sub/node-status-loading?])]
    (when (and (some? loading?) (not loading?))
      [:div.node-info__container
       [:div.node-info
        [:h2.node-info__title "Node Status"]
        [status-map->html-elements status-map]]
       [:div.node-info
        [:h2.node-info__title "Node Options"]
        [status-map->html-elements options-map]]])))

(defn root-page
  []
  [:div.root-page
   [:div.root-background]
   [:div.root-contents
    [:h1.root-title "Welcome to the Crux Console!"]
    [:h2.root-video__title "Take a short video tour:"]
    [:iframe.root-video {:src "https://www.youtube.com/embed/StXLmWvb5Xs"
                         :allow "accelerometer; autoplay; encrypted-media; gyroscope; picture-in-picture"}]
    [:a.root-get-started
     {:href (common/route->url :query)}
     "Start exploring your Crux node"]]])

(defn view []
  (let [{{:keys [name]} :data} @(rf/subscribe [::sub/current-route])]
    [:<>
     #_[:pre (with-out-str (pprint/pprint (dissoc @(rf/subscribe [:db]) :query)))]
     [:div.container.page-pane
      (cond
        (= name :homepage) [root-page]
        (= name :status) [status-page]
        :else [:<>
               (when name [form-pane])
               [:div.result-pane
                (case name
                  :query [query-pane]
                  :entity [entity-pane]
                  [:div "no matching"])]])]]))
