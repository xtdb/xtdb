(ns crux.ui.views
  (:require
   [clojure.pprint :as pprint]
   [clojure.string :as string]
   [crux.ui.events :as events]
   [crux.ui.subscriptions :as sub]
   [crux.ui.uikit.table :as table]
   [fork.core :as fork]
   [reagent.core :as r]
   [re-frame.core :as rf]
   [tick.alpha.api :as t]))

(defn query-box
  []
  (let [now-date (t/date)
        now-time (t/time)
        search-view @(rf/subscribe [::sub/search-view])]
    [fork/form {:path :query
                :form-id "query"
                :prevent-default? true
                :clean-on-unmount? true
                :initial-values {"valid-date" now-date
                                 "valid-time" now-time}
                :on-submit #(if (= :query search-view)
                              (rf/dispatch [::events/go-to-query %])
                              (rf/dispatch [::events/go-to-entity %]))}
     (fn [{:keys [values
                  state
                  form-id
                  handle-change
                  handle-blur
                  submitting?
                  handle-submit]}]
       [:<>
        [:form
         {:id form-id
          :on-submit handle-submit}
         [:textarea.textarea
          {:name "q"
           :value (get values "q")
           :on-change handle-change
           :on-blur handle-blur
           :cols 40
           :rows 10}]
         [:div.crux-time
          [:div.input-group.valid-time
           [:div.label
            [:label "Valid Time"]]
           [:input.input {:type "date"
                          :name "valid-date"
                          :value (get values "valid-date")
                          :on-change handle-change
                          :on-blur handle-blur}]
           [:input.input {:type "time"
                          :name "valid-time"
                          :step "any"
                          :value (get values "valid-time")
                          :on-change handle-change
                          :on-blur handle-blur}]]
          [:div.input-group
           [:div.label
            [:label "Transaction Time"]]
           [:input.input {:type "date"
                          :name "transaction-date"
                          :value (get values "transaction-date")
                          :on-change handle-change
                          :on-blur handle-blur}]
           [:input.input {:type "time"
                          :name "transaction-time"
                          :value (get values "transaction-time")
                          :step "any"
                          :on-change handle-change
                          :on-blur handle-blur}]]]
         [:button.button
          {:type "submit"}
          "Submit Query"]]])]))

(defn query-table
  []
  (let [{:keys [error data]} @(rf/subscribe [::sub/query-data-table])]
    [:<>
     (if error
       [:div.error-box error]
       [table/table data])]))

(defn query-view
  []
  (let [query-view @(rf/subscribe [::sub/query-view])]
    [:<>
     (case query-view
       :table [query-table])]))

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
  (let [{:keys [linked-entities entity-result entity-name vt tt]}
        @(rf/subscribe [::sub/entity-view-data])
        loading? @(rf/subscribe [::sub/entity-loading?])]
    [:div.entity-map__container
     (if loading?
       [:div.entity-map.entity-map--loading
        [:i.fas.fa-spinner.entity-map__load-icon]]
       [:<>
        [:div.entity-map
         (if entity-result
           [:<>
            [:div.entity-group
             [:div.entity-group__key
              ":crux.db/id"]
             [:div.entity-group__value
              (str (:crux.db/id entity-result))]]
            [:hr.entity-group__separator]
            (entity->hiccup linked-entities
                            (dissoc entity-result :crux.db/id))]
           [:<> [:strong entity-name] " entity not found"])]
        [:div.entity-vt-tt
         [:div.entity-vt-tt__title
          "Valid Time"]
         [:div.entity-vt-tt__value vt]
         [:div.entity-vt-tt__title
          "Transaction Time"]
         [:div.entity-vt-tt__value tt]]])]))

(defn entity-view
  []
  (let [entity-view @(rf/subscribe [::sub/entity-view])]
    [:<>
     [:div.pane-nav
      [:div.pane-nav__tab
       {:class (if (= entity-view :document)
                 "pane-nav__tab--active"
                 "pane-nav__tab--hover")
        :on-click #(rf/dispatch [::events/set-entity-view :document])}
       "Document"]]
     (case entity-view
       :document [entity-document]
       :history [:div "this is history"])]))

(defn left-pane
  []
  (let [query-pane-show? @(rf/subscribe [::sub/query-pane-show?])
        search-view @(rf/subscribe [::sub/search-view])]
    [:div.left-pane
     (if query-pane-show?
       [:div.hide-button
        {:on-click #(rf/dispatch [::events/query-pane-toggle])}
        "Hide"]
       [:button.button.hidden-pane
        {:on-click #(rf/dispatch [::events/query-pane-toggle])}
        [:span "."]
        [:span "."]
        [:span "."]])
     [:div
      {:class (if query-pane-show?
                "pane-toggled"
                "pane-untoggled")}
      [:div.pane-nav
       [:div.pane-nav__tab
        {:class (if (= search-view :query)
                  "pane-nav__tab--active"
                  "pane-nav__tab--hover")
         :on-click #(rf/dispatch [::events/set-search-view :query])}
        "Query"]
       [:div.pane-nav__tab
        {:class (if (= search-view :entity)
                  "pane-nav__tab--active"
                  "pane-nav__tab--hover")
         :on-click #(rf/dispatch [::events/set-search-view :entity])}
        "Entity"]]
      [query-box]]]))

(defn view []
  (let [{:keys [handler]} @(rf/subscribe [::sub/current-page])]
    [:div.container.page-pane
     [left-pane]
     [:div.right-pane
      [:div.back-button
       [:a
        {:on-click #(js/window.history.back)}
        [:i.fas.fa-chevron-left]
        [:span.back-button__text "Back"]]]
      (case handler
        :query [query-view]
        :entity [entity-view]
        [:div "no matching"])]]))
