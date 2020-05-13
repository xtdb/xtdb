(ns crux.ui.views
  (:require
   [clojure.string :as string]
   [clojure.pprint :as pprint]
   [crux.ui.events :as events]
   [crux.ui.common :as common]
   [crux.ui.subscriptions :as sub]
   [crux.ui.uikit.table :as table]
   [fork.core :as fork]
   [reagent.core :as r]
   [reitit.core :as reitit]
   [re-frame.core :as rf]
   [tick.alpha.api :as t]))

(defn vt-tt-inputs
  [{:keys [values touched errors handle-change handle-blur]}]
  [:div.crux-time
   [:div.input-group
    [:div.label
     [:label "Valid Time"]]
    [:input.input.input-time
     {:type "date"
      :name "vtd"
      :value (get values "vtd")
      :on-change handle-change
      :on-blur handle-blur}]
    [:input.input
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
    [:div.label
     [:label "Transaction Time" ]]
    [:input.input.input-time
     {:type "date"
      :name "ttd"
      :value (get values "ttd")
      :on-change handle-change
      :on-blur handle-blur}]
    [:input.input
     {:type "time"
      :name "ttt"
      :value (get values "ttt")
      :on-change handle-change
      :on-blur handle-blur}]
    (when (and (or (get touched "ttd")
                   (get touched "ttt"))
               (get errors "tt"))
      [:p.input-error (get errors "tt")])]])

(defn query-validation
  [values]
  (let [invalid? #(empty? (string/trim (or (get values %) "")))
        validation {"q" (when (invalid? "q") "Query box is empty")
                    "vt" (when (apply not= ((juxt #(% "vtd")
                                                  #(% "vtt")) invalid?))
                           "Fill out both inputs or none")
                    "tt" (when (apply not= ((juxt #(% "ttd")
                                                  #(% "ttt")) invalid?))
                           "Fill out both inputs or none")}]
    (when (some some? (vals validation)) validation)))

(defn query-form
  []
  [fork/form {:form-id "form-query"
              :validation query-validation
              :prevent-default? true
              :clean-on-unmount? true
              :initial-values @(rf/subscribe [::sub/initial-values-query])
              :on-submit #(rf/dispatch [::events/go-to-query-view %])}
   (fn [{:keys [values
                state
                errors
                touched
                form-id
                handle-change
                handle-blur
                handle-submit] :as props}]
     (let [loading? @(rf/subscribe [::sub/query-right-pane-loading?])
           query-pane? (= :query @(rf/subscribe [::sub/left-pane-view]))]
       [:form.hidden
        {:class (when query-pane? "visible")
         :id form-id
         :on-submit handle-submit}
        [:div.input-group
         [:textarea.textarea
          {:name "q"
           :value (get values "q")
           :on-change handle-change
           :on-blur handle-blur
           :rows 10}]
         (when (and (get touched "q")
                    (get errors "q"))
           [:p.input-error (get errors "q")])]
        [vt-tt-inputs props]
        [:button.button
         {:type "submit"
          :disabled (or loading? (some some? (vals errors)))}
         "Submit Query"]]))])

(defn entity-validation
  [values]
  (let [invalid? #(empty? (string/trim (or (get values %) "")))
        validation {"eid" (when (invalid? "eid") "Entity id is empty")
                    "vt" (when (apply not= ((juxt #(% "vtd")
                                                  #(% "vtt")) invalid?))
                           "Fill out both inputs or none")
                    "tt" (when (apply not= ((juxt #(% "ttd")
                                                  #(% "ttt")) invalid?))
                           "Fill out both inputs or none")}]
    (when (some some? (vals validation)) validation)))

(defn entity-form
  []
  [fork/form {:form-id "form-entity"
              :prevent-default? true
              :clean-on-unmount? true
              :validation entity-validation
              :initial-values @(rf/subscribe [::sub/initial-values-entity])
              :on-submit #(rf/dispatch [::events/go-to-entity-view %])}
   (fn [{:keys [values
                touched
                errors
                form-id
                state
                handle-change
                handle-blur
                handle-submit] :as props}]
     (let [loading? @(rf/subscribe [::sub/entity-right-pane-loading?])
           entity-pane? (= :entity @(rf/subscribe [::sub/left-pane-view]))]
       [:form.hidden
        {:class (when entity-pane? "visible")
         :id form-id
         :on-submit handle-submit}
        [:div.input-group
         [:textarea.textarea
          {:name "eid"
           :value (get values "eid")
           :on-change handle-change
           :on-blur handle-blur}]
         (when (and (get touched "eid")
                    (get errors "eid"))
           [:p.input-error (get errors "eid")])]
        [vt-tt-inputs props]
        [:button.button
         {:type "submit"
          :disabled (or loading? (some some? (vals errors)))}
         "Submit Entity"]]))])

(defn query-table
  []
  (let [{:keys [error data]} @(rf/subscribe [::sub/query-data-table])]
    [:<>
     (if error
       [:div.error-box error]
       [table/table data])]))

(defn query-right-pane
  []
  (let [right-pane-view @(rf/subscribe [::sub/query-right-pane-view])]
    [:<>
     [:div.pane-nav
      [:div.pane-nav__tab
       "Table"]
      #_[:div.pane-nav__tab
       {:class (if (= right-pane-view :graph)
                 "pane-nav__tab--active"
                 "pane-nav__tab--hover")
        :on-click #(rf/dispatch [::events/set-query-right-pane-view :graph])}
         "Graph"]]
     (case right-pane-view
       :table [query-table]
       :graph [:div "this is graph"]
       nil)]))

(defn- entity->hiccup
  [links edn]
  (if-let [href (get links edn)]
    [:a.entity-link
     {:href href}
     (str edn)]
    (cond
      (map? edn) (for [[k v] edn]
                   ^{:key (str (gensym))}
                   [:div.entity-group
                    [:div.entity-group__key
                     (entity->hiccup links k)]
                    [:div.entity-group__value
                     (entity->hiccup links v)]])
      (sequential? edn) [:ol.entity-group__value
                         (for [v edn]
                           ^{:key (str (gensym))}
                           [:li (entity->hiccup links v)])]
      (set? edn) [:ul.entity-group__value
                  (for [v edn]
                    ^{:key v}
                    [:li (entity->hiccup links v)])]
      :else (str edn))))

(defn entity-document
  []
  (let [{:keys [eid vt tt document document-no-eid linked-entities]}
        @(rf/subscribe [::sub/entity-right-pane-document])
        loading? @(rf/subscribe [::sub/entity-right-pane-loading?])]
    [:div.entity-map__container
     (if loading?
       [:div.entity-map.entity-map--loading
        [:i.fas.fa-spinner.entity-map__load-icon]]
       [:<>
        [:div.entity-map
         (if document
           [:<>
            [:div.entity-group
             [:div.entity-group__key
              ":crux.db/id"]
             [:div.entity-group__value (str eid)]]
            [:hr.entity-group__separator]
            (entity->hiccup linked-entities document-no-eid)]
           [:<> [:strong eid] " entity not found"])]
        [:div.entity-vt-tt
         [:div.entity-vt-tt__title
          "Valid Time"]
         [:div.entity-vt-tt__value vt]
         [:div.entity-vt-tt__title
          "Transaction Time"]
         [:div.entity-vt-tt__value tt]]])]))

(defn entity-right-pane
  []
  (let [right-pane-view @(rf/subscribe [::sub/entity-right-pane-view])]
    [:<>
     [:div.pane-nav
      [:div.pane-nav__tab
       "Document"]
      #_[:div.pane-nav__tab
       {:class (if (= right-pane-view :history)
                 "pane-nav__tab--active"
                 "pane-nav__tab--hover")
        :on-click #(rf/dispatch [::events/set-entity-right-pane-view :history])}
       "History"]]
     (case right-pane-view
       :document [entity-document]
       :history [:div "this is history"]
       nil)]))

(defn left-pane
  []
  (let [left-pane-visible? @(rf/subscribe [::sub/left-pane-visible?])
        left-pane-view @(rf/subscribe [::sub/left-pane-view])]
    [:div.left-pane
     (if left-pane-visible?
       [:div.hide-button
        {:on-click #(rf/dispatch [::events/toggle-left-pane])}
        "Hide"]
       [:button.button.hidden-pane
        {:on-click #(rf/dispatch [::events/toggle-left-pane])}
        [:span "."]
        [:span "."]
        [:span "."]])
     [:div
      {:class (if left-pane-visible?
                "pane-toggled"
                "pane-untoggled")}
      [:div.pane-nav
       [:div.pane-nav__tab
        {:class (if (= left-pane-view :query)
                  "pane-nav__tab--active"
                  "pane-nav__tab--hover")
         :on-click #(rf/dispatch [::events/set-left-pane-view :query])}
        "Query"]
       [:div.pane-nav__tab
        {:class (if (= left-pane-view :entity)
                  "pane-nav__tab--active"
                  "pane-nav__tab--hover")
         :on-click #(rf/dispatch [::events/set-left-pane-view :entity])}
        "Entity"]]
      [query-form]
      [entity-form]]]))

(defn view []
  (let [{{:keys [name]} :data} @(rf/subscribe [::sub/current-route])]
    [:<>
     #_[:pre (with-out-str (pprint/pprint (dissoc @(rf/subscribe [:db]) :query)))]
     [:div.container.page-pane
      (when name [left-pane])
      [:div.right-pane
       [:div.back-button
        [:a
         {:on-click common/back-page}
         [:i.fas.fa-chevron-left]
         [:span.back-button__text "Back"]]]
       (case name
         :query [query-right-pane]
         :entity [entity-right-pane]
         [:div "no matching"])]]]))
