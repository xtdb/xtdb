(ns example-standalone-webservice.main
  (:require [crux.api :as api]
            [crux.http-server :as srv]
            [crux.backup :as backup]
            [crux.codec :as c]
            [crux.io :as crux-io]
            [clojure.instant :as instant]
            [clojure.tools.logging :as log]
            [clojure.pprint :as pp]
            [clojure.string :as str]
            [clojure.spec.alpha :as s]
            [yada.yada :refer [handler listener]]
            [hiccup2.core :refer [html]]
            [hiccup.util]
            [yada.resource :refer [resource]]
            [yada.resources.classpath-resource]
            [cheshire.core :as json]
            [clojure.java.io :as io]
            [clojure.java.shell :refer [sh]])
  (:import [crux.api IndexVersionOutOfSyncException]
           java.io.Closeable
           [java.util Calendar Date TimeZone UUID]
           java.time.ZoneId
           java.text.SimpleDateFormat))

(defn- format-date [^Date d]
  (when d
    (.format ^SimpleDateFormat (.get ^ThreadLocal @#'instant/thread-local-utc-date-format) d)))

(defn- with-date-links [tt-str x]
  (hiccup.util/raw-string
   (str/replace x
                #"\#inst \"(.+)\""
                (fn [[s d]]
                  (str/replace s d (str "<a href=\"/?vt=" d (when tt-str
                                                              (str "&tt=" tt-str)) "\">" d "</a>"))))))

(defn- page-head [title]
  [:head
   [:meta {:charset "utf-8"}]
   [:meta {:http-equiv "Content-Language" :content "en"}]
   [:meta {:name "google" :content "notranslate"}]
   [:link {:rel "stylesheet" :type "text/css" :href "/static/styles/normalize.css"}]
   [:link {:rel "stylesheet" :type "text/css" :href "/static/styles/main.css"}]
   [:link {:rel "stylesheet" :type "text/css" :href "/cljsjs/codemirror/production/codemirror.min.css"}]
   [:link {:rel "stylesheet" :type "text/css" :href "/cljsjs/codemirror/common/theme/eclipse.css"}]
   [:script {:src "/cljsjs/production/vega.min.inc.js"}]
   [:script {:src "/cljsjs/production/vega-lite.min.inc.js"}]
   [:script {:src "/cljsjs/production/vega-embed.min.inc.js"}]
   [:script {:src "/cljsjs/codemirror/production/codemirror.min.inc.js"}]
   [:script {:src "/cljsjs/codemirror/common/mode/clojure.inc.js"}]
   [:script {:src "/cljsjs/codemirror/common/keymap/emacs.inc.js"}]
   [:script {:src "/cljsjs/codemirror/common/addon/edit/closebrackets.inc.js"}]
   [:script {:src "/cljsjs/codemirror/common/addon/edit/matchbrackets.inc.js"}]
   [:title title]])

(defn- vega-graph [element-id tx-log min-time max-time now max-known-tt vt tt width height]
  (let [known-times (for [{:crux.tx/keys [tx-time tx-ops tx-id]} tx-log
                          command tx-ops
                          x command
                          :when (inst? x)]
                      {:vt x
                       :tt tx-time
                       :tx-id tx-id
                       :url (str "?vt=" (format-date x) "&" "tt=" (format-date tx-time))})
        vega-spec {:width width
                   :height height

                   :layer [{:data {:values [{:vt now :tt now :event "NOW"
                                             :url (str "?vt=" (format-date now) "&" "tt=" (format-date now))
                                             :color "lightgreen"}
                                            {:vt max-known-tt  :tt max-known-tt :event "MAX"
                                             :url (str "?vt=" (format-date max-known-tt) "&" "tt=" (format-date max-known-tt))
                                             :color "grey"}
                                            {:vt vt :tt tt :event "selection"
                                             :url (str "?vt=" (format-date vt) "&" "tt=" (format-date tt))
                                             :color "blue"}]}
                            :layer [{:mark :rect
                                     :encoding {:x {:value min-time :type :temporal :timeUnit "utcyearmonthdatehoursminutesseconds"}
                                                :x2 {:field :tt :type :temporal :timeUnit "utcyearmonthdatehoursminutesseconds"}
                                                :y {:value min-time :type :temporal :timeUnit "utcyearmonthdatehoursminutesseconds"}
                                                :y2 {:field :vt :type :temporal :timeUnit "utcyearmonthdatehoursminutesseconds"}
                                                :fillOpacity {:value 0.25}
                                                :strokeOpacity {:value 1}
                                                :strokeWidth {:value 2}
                                                :stroke {:field :color :type :nominal :scale nil}
                                                :fill  {:field :color :type :nominal :scale nil}
                                                :tooltip [{:field :event :type :nominal
                                                           :title "Event"}
                                                          {:field :vt :type :temporal
                                                           :title "VT"
                                                           :timeUnit "utcyearmonthdatehoursminutesseconds"}
                                                          {:field :tt :type :temporal
                                                           :title "TT"
                                                           :timeUnit "utcyearmonthdatehoursminutesseconds"}]}}
                                    {:mark :rule
                                     :encoding {:x {:field :tt :type :temporal :timeUnit "utcyearmonthdatehoursminutesseconds"}
                                                :color {:field :color :type :nominal :scale nil}
                                                :tooltip [{:field :event :type :nominal
                                                           :title "Event"}
                                                          {:field :tt :type :temporal
                                                           :title "TT"
                                                           :timeUnit "utcyearmonthdatehoursminutesseconds"}]
                                                :href {:field :url :type :nominal}}}]}
                           {:mark :circle
                            :data {:values known-times}
                            :selection {:bitemp_selection {:type :interval}}
                            :encoding {:x {:field :tt
                                           :type :temporal
                                           :timeUnit "utcyearmonthdatehoursminutesseconds"
                                           :scale {:domain [min-time max-time]}
                                           :axis {:title "Transaction Time"}}
                                       :y {:field :vt
                                           :type :temporal
                                           :scale {:domain [min-time max-time]}
                                           :axis {:title "Valid Time"}}
                                       :tooltip [{:field :tx-id :type :ordinal
                                                  :title "tx-id"}
                                                 {:field :vt :type :temporal
                                                  :title ":VT"
                                                  :timeUnit "utcyearmonthdatehoursminutesseconds"}
                                                 {:field :tt :type :temporal
                                                  :title "TT"
                                                  :timeUnit "utcyearmonthdatehoursminutesseconds"}]
                                       :href {:field :url :type :nominal}}}]}]
    [:script {:type "text/javascript"}
     (hiccup.util/raw-string
      (format "
     vegaEmbed('%s', %s).then(function(result)  {
        result.view.addEventListener('click', function(event, item) {
           var datum = item && item.datum;
           console.log(event, item, datum && datum.a, datum && datum.b);
        });
        result.view.addDataListener('bitemp_selection_store', function(name, value) {
           console.log(name, value);
        });
     });"

              "#vega"
              (json/generate-string vega-spec)))]))

(defn- footer []
  [:footer
   "© 2019 "
   [:a {:href "https://juxt.pro"} "JUXT"]])

(defn- status-block [crux]
  [:div.status
   [:h4 "Status:"]
   [:pre.edn (with-date-links nil (with-out-str
                                (pp/pprint (api/status crux))))]])

(defn- valid-time-link [vt]
  (let [vt-str (format-date vt)]
    [:a {:href (str "/?vt=" vt-str)} vt-str]))

(defn- draw-timeline-graph [tx-log min-time max-time now max-known-tt vt tt width height]
  (let [known-times (for [{:crux.tx/keys [tx-time tx-ops]} tx-log
                          command tx-ops
                          x command
                          :when (inst? x)]
                      {:vt x :tt tx-time})
        time-diff (double (- (inst-ms max-time) (inst-ms min-time)))
        min-time-str (format-date min-time)
        max-time-str (format-date max-time)
        now-str (format-date now)
        tt (or tt now)
        time->x (fn [t]
                  (* width (/ (- (inst-ms t) (inst-ms min-time)) time-diff)))
        onclick-timeline-js "window.location = ('/?%s=' + Math.round(%f * (window.event.offsetX / window.event.target.getBoundingClientRect().width) + %d) + '&%s=%d');"]
    [:svg.timeline-graph {:version "1.1" :xmlns "http://www.w3.org/2000/svg" :viewBox (str "0 0 " width " " height)}
     [:a {:href (str "/?vt=" min-time-str)} [:text.min-time {:x 0 :y (* 0.55 height)} min-time-str]]
     [:a {:href (str "/?vt=" max-time-str)} [:text.max-time {:x width :y (* 0.55 height)} max-time-str]]
     (when max-known-tt
       [:a.max-transaction-time {:href (str "/?tt=" (format-date max-known-tt))}
        [:text {:x (* 0.995 (time->x max-known-tt)) :y (* 0.85 height)} "MAX"]
        [:line {:x1 (time->x max-known-tt) :y1 (* 0.25 height) :x2 (time->x max-known-tt) :y2 (* 0.75 height)}]])
     [:a.time-horizon {:href "/"}
      [:text {:x (* 1.005 (time->x now)) :y (* 0.85 height)} "NOW"]
      [:line {:x1 (time->x now) :y1 (* 0.1 height) :x2 (time->x now) :y2 (* 0.9 height)}]]
     [:g.axis
      [:text.axis-name {:x 0 :y (* 0.2 height)} "VT: "
       [:tspan.axis-value (format-date vt)]]
      [:line.axis-line {:x1 0 :y1 (* 0.25 height) :x2 width :y2 (* 0.25 height) :stroke-width (* 0.01 height)
                        :onclick (format onclick-timeline-js "vt" time-diff (inst-ms min-time) "tt" (inst-ms tt))}]
      (for [{:keys [tt vt]} (sort-by :vt known-times)
            :let [vt-str (format-date vt)
                  x (time->x vt)]]
        [:a.timepoint {:href (str "/?vt=" vt-str "&tt=" (format-date tt))}
         [:g
          [:line.timepoint-marker {:x1 x :y1 (* 0.2 height) :x2 x :y2 (* 0.3 height)}]
          [:text {:x x :y (* 0.15 height)} (str vt-str " | " (format-date tt))]]])]
     [:g.axis
      [:line.axis-line {:x1 0 :y1 (* 0.75 height) :x2 width :y2 (* 0.75 height) :stroke-width (* 0.01 height)
                        :onclick (format onclick-timeline-js "tt" time-diff (inst-ms min-time) "vt" (inst-ms vt))}]
      (for [tt (sort (map :tt known-times))
            :let [tt-str (format-date tt)
                  x (time->x tt)]]
        [:a.timepoint {:href (str "/?tt=" tt-str)}
         [:g
          [:line.timepoint-marker {:x1 x :y1 (* 0.7 height) :x2 x :y2 (* 0.8 height)}]
          [:text {:x x :y (* 0.65 height)} tt-str]]])
      [:text.axis-name {:x 0 :y (* 0.9 height)} "TT: "
       [:tspan.axis-value (or (format-date tt) "empty")]]]
     (when tt
       [:line.bitemp-coordinates {:x1 (time->x vt) :y1 (* 0.25 height) :x2 (time->x tt) :y2 (* 0.75 height)}])]))

;; TODO: WIP.
(defn- draw-timeline-2d-graph [tx-log min-time max-time now max-known-tt vt tt width height]
  (let [known-times (for [{:crux.tx/keys [tx-time tx-ops]} tx-log
                          command tx-ops
                          x command
                          :when (inst? x)]
                      {:vt x :tt tx-time})
        time-diff (double (- (inst-ms max-time) (inst-ms min-time)))
        min-time-str (format-date min-time)
        max-time-str (format-date max-time)
        now-str (format-date now)
        tt (or tt now)
        time->x (fn [t]
                  (* width (/ (- (inst-ms t) (inst-ms min-time)) time-diff)))
        time->y (fn [t]
                  (- height (* height (/ (- (inst-ms t) (inst-ms min-time)) time-diff))))
        onclick-timeline-js (format "window.location = '?tt=' + Math.round((%f * (window.event.offsetX / window.event.target.getBoundingClientRect().width)) + %d) + '&vt=' +  Math.round(%f * ((window.event.target.getBoundingClientRect().height - window.event.offsetY) / (window.event.target.getBoundingClientRect().height)) + %d);"

                                    time-diff (inst-ms min-time) time-diff (inst-ms min-time))]
    [:svg#timeline-graph.timeline-graph.timeline-2d-graph
     {:version "1.1" :xmlns "http://www.w3.org/2000/svg" :viewBox (str "0 0 " width " " height)
      :onclick onclick-timeline-js}
     (let [x (time->x tt)
           y (time->y vt)]
       [:g.bitemp-coordinates
        [:rect {:x 1 :y y :width x :height (- height y) :pointer-events "none"}]
        [:text {:x x :y (+ (* 0.025 height) y) :pointer-events "none"} (format-date vt) " | " (format-date tt)]])
     [:a {:href (str "?vt=" min-time-str)} [:text.min-time {:x (* 0.01 width) :y (* 0.985 height)} min-time-str]]
     [:a {:href (str "?vt=" max-time-str)} [:text.max-time {:x width :y (* 0.015 height)} max-time-str]]
     [:line.time-arrow {:x1 1 :y1 height :x2  width :y2 1}]
     (when max-known-tt
       (let [x (time->x max-known-tt)
             y (time->y max-known-tt)]
         [:a.max-transaction-time {:href (str "?tt=" (format-date max-known-tt))}
          [:text {:x x :y y} "MAX"]
          [:line {:x1 x :y1 1
                  :x2 x :y2 height}]]))
     (let [x (time->x now)
           y (time->y now)]
       [:a.time-horizon {:href "/timeline"}
        [:text {:x x :y y} "NOW"]
        [:rect {:x 1 :y y :width x :height (- height y) :pointer-events "stroke"}]])
     (for [{:keys [tt vt]} (sort-by :vt known-times)
           :let [vt-str (format-date vt)
                 x (time->x tt)
                 y (time->y vt)]]
       [:a.timepoint {:href (str "?vt=" vt-str "&tt=" (format-date tt))}
        [:g
         [:circle.timepoint-marker {:cx x :cy y :r 2}]
         [:text {:x x :y (+ (* 0.025 height) y) :pointer-events "none"} (str vt-str " | " (format-date tt))]]])
     [:g.axis.vt
      [:text.axis-name {:x (* 0.015 width) :y (* 0.015 height)} "VT"]
      [:line.axis-line {:x1 1 :y1 0 :x2 1 :y2 height :pointer-events "none"}]]
     [:g.axis.tt
      [:text.axis-name {:x (* 0.985 width) :y (* 0.985 height)} "TT"]
      [:line.axis-line {:x1 0 :y1 (dec height) :x2 width :y2 (dec height) :pointer-events "none"}]]]))

(defn- parse-query-date [d]
  (if (re-find #"^\d+$" d)
    (Date. (Long/parseLong d))
    (instant/read-instant-date d)))

(defn- min-max-time [^Date from]
  (let [utc (ZoneId/of "UTC")
        ld (.toLocalDate (.atZone (.toInstant from) utc))]
    [(Date/from (.toInstant (.atStartOfDay (.minusDays ld 0) utc)))
     (Date/from (.toInstant (.atStartOfDay (.plusDays ld 1) utc)))]))

(defn- time-context [crux ctx]
  (let [{:strs [vt tt]} (get-in ctx [:parameters :query])
        now (Date.)
        vt (if (not (str/blank? vt))
             (parse-query-date vt)
             now)
        tt (when (not (str/blank? tt))
             (parse-query-date tt))
        tx-log (with-open [tx-log-cxt (api/new-tx-log-context crux)]
                 (vec (api/tx-log crux tx-log-cxt nil true)))
        max-known-tt (:crux.tx/tx-time (last tx-log))
        tt (or tt max-known-tt)
        tt (if (pos? (compare tt max-known-tt))
             max-known-tt
             tt)
        db (api/db crux vt tt)
        [min-time max-time] (min-max-time now)]
    {:vt vt :tt tt :now now :tx-log tx-log :max-known-tt max-known-tt :min-time min-time :max-time max-time :db db}))

(defn index-handler
  [ctx {:keys [crux]}]
  (fn [ctx]
    (let [{:keys [vt tt now tx-log max-known-tt min-time max-time db]} (time-context crux ctx)
          grow-textarea-oninput-js "this.style.height = ''; this.style.height = this.scrollHeight + 'px';"]
      (str
       "<!DOCTYPE html>"
       (html
        [:html {:lang "en"}
         (page-head "Message Board")
         [:body
          [:header
           [:h2 [:a {:href "/"} "Message Board"]]]
          [:div.timetravel
           (draw-timeline-graph tx-log min-time max-time now max-known-tt vt tt 750 100)]
          [:div.comments
           [:h3 "Comments"]
           [:ul
            (for [[message name created id]
                  (->> (api/q db
                              '{:find [m n c e]
                                :where [[e :message-post/message m]
                                        [e :message-post/name n]
                                        [e :message-post/created c]]})
                       (sort-by #(nth % 2)))]
              [:li.comment
               [:span.comment-meta
                name
                " at "
                (valid-time-link created)
                (let [history (with-open [snapshot ^Closeable (api/new-snapshot db)]
                                (mapv :crux.db/valid-time (api/history-descending db snapshot id)))
                      history-onchange-js "this.form.submit();"]
                  (when (> (count history) 1)
                    [:span " • "
                     [:form.version-history {:method "GET" :action "/"  :autocomplete "off"}
                      [:label {:for "version-history-list"} "edited at "]
                      [:select#version-history-list {:name "vt" :onchange history-onchange-js :placeholder "versions:"}
                       (for [history-vt history
                             :let [vt-str (format-date history-vt)]]
                         [:option {:value vt-str :selected (= vt history-vt)} vt-str])]]]))]
               [:form.edit-comment {:action (str "/comment/" id) :method "POST" :autocomplete "off"}
                [:fieldset
                 [:input {:type "hidden" :name "created" :value (format-date created)}]
                 [:input {:type "hidden" :name "name" :value name}]
                 [:textarea {:id (str "edit-message-" id) :rows (count (str/split-lines message)) :name "message" :required true
                             :oninput grow-textarea-oninput-js} message]]
                [:div.buttons
                 [:input.primary {:type "submit" :name "_action" :value "Edit"}]
                 [:input {:type "submit" :name "_action" :value "Delete"}]
                 [:input {:type "submit" :name "_action" :value "Delete History"}]
                 [:input {:type "submit" :name "_action" :value "Evict"}]]]])]

           [:div.add-comment-box
            [:h5 "Add new comment"]
            [:form {:action "/comment" :method "POST" :autocomplete "off"}
             [:fieldset
              [:input {:type "hidden" :name "created" :value (format-date vt)}]
              [:input {:type "text" :id "name" :name "name" :required true :placeholder "Name"}]
              [:textarea {:cols "40" :rows "10" :id "message" :name "message" :required true :placeholder "Message"}]]
             [:input.primary {:type "submit" :name "_action" :value "Comment"}]
             [:input.primary {:type "submit" :name "_action" :value "Bitemporal Comment"}]]]]

          [:h5
           [:a {:href "tx-log"} "Transaction History"]]
          [:h5
           [:a {:href (str "timeline?vt=" (format-date vt) "&tt=" (format-date tt))} "Timeline Graph"]]
          [:h5
           [:a {:href (str "query?vt=" (format-date vt) "&tt=" (format-date tt))} "Query Console"]]
          (status-block crux)
          (footer)]])))))

(defn tx-log-handler [ctx {:keys [crux]}]
  (fn [ctx]
    (let [tx-log (with-open [tx-log-cxt (api/new-tx-log-context crux)]
                   (vec (api/tx-log crux tx-log-cxt nil true)))]
      (str
       "<!DOCTYPE html>"
       (html
        [:html
         (page-head "Message Board - Transaction History")
         [:body
          [:header
           [:h2 [:a {:href ""} "Transaction History"]
            [:small "(Earliest first.)"]]]
          [:div.transaction-history
           [:table
            [:thead
             [:th (str :crux.tx/tx-id)]
             [:th (str :crux.tx/tx-time)]
             [:th (str :crux.api/tx-ops)]]
            [:tbody
             (for [{:crux.tx/keys [tx-id tx-time tx-ops]} tx-log
                   :let [tx-time-str (format-date tx-time)]]
               [:tr
                [:td {:id (str tx-id)} tx-id]
                [:td [:a {:href (str "/?tt=" tx-time-str)} tx-time-str]]
                [:td (with-date-links (format-date tx-time)
                       (with-out-str
                         (pp/pprint tx-ops)))]])]]]
          [:h5
           [:a {:href "/"} "Back to Message Board"]]
          (status-block crux)
          (footer)]])))))

(defn- pp-entity-tx [entity-tx]
  (let [content-hash (:crux.db/content-hash entity-tx)
        eid (:crux.db/id entity-tx)
        tx-id (:crux.tx/tx-id entity-tx)]
    (-> (with-out-str
          (pp/pprint entity-tx))
        (str/replace
         eid
         (format "<a href=\"/entity/%s\">%s</a>" eid eid))
        (str/replace
         content-hash
         (format "<a href=\"/document/%s\">%s</a>" content-hash content-hash))
        (str/replace
         (str tx-id)
         (format "<a href=\"/tx-log#%s\">%s</a>" tx-id tx-id)))))

(defn- pp-entity-txs [entity-txs]
  (for [{:crux.tx/keys [tx-id tx-time] :as entity-tx} entity-txs
        :let [tx-time-str (format-date tx-time)]]
    (with-date-links (format-date tx-time)
      (pp-entity-tx entity-tx))))

(defn timeline-graph-handler [ctx {:keys [crux]}]
  (fn [ctx]
    (let [{:keys [vt tt now tx-log max-known-tt min-time max-time db]} (time-context crux ctx)
          eids (api/q db '{:find [e]
                           :where [[e :message-post/message]]})
          entity-txs (->> (for [[eid] eids]
                            (api/history-range crux eid nil nil vt tt))
                          (reduce into [])
                          (sort-by (juxt :crux.db/valid-time :crux.tx/tx-time)))]
      (str
       "<!DOCTYPE html>"
       (html
        [:html
         (page-head "Message Board - Timeline Graph")
         [:body
          [:header
           [:h2 [:a {:href ""} "Timeline Graph"]]]
          [:div.timetravel
           (draw-timeline-2d-graph tx-log min-time max-time now max-known-tt vt tt 500 500)]
          [:div#vega]
          [:div
           [:h4 "Entity TXs"]
           [:pre.edn (pp-entity-txs entity-txs)]]
          [:h5
           [:a {:href (str "/?tt=" (format-date tt) "&vt=" (format-date vt))} "Back to Message Board"]]
          #_(vega-graph "#vega" tx-log min-time max-time now max-known-tt vt tt 750 750)
          (status-block crux)
          (footer)]])))))

(defn query-handler [ctx {:keys [crux]}]
  (fn [ctx]
    (let [{:keys [vt tt now tx-log max-known-tt min-time max-time db]} (time-context crux ctx)
          {:strs [q]} (get-in ctx [:parameters :query])
          _ (prn q)
          conformed-query (s/conform :crux.query/query q)
          query-invalid? (= :clojure.spec.alpha/invalid conformed-query)
          start-time (System/currentTimeMillis)
          result (when-not query-invalid?
                   (api/q db q))
          query-time (- (System/currentTimeMillis) start-time)
          invalid? (and query-invalid? (not (str/blank? q)))
          grow-textarea-oninput-js "this.style.height = ''; this.style.height = this.scrollHeight + 'px';"
          ctrl-enter-to-submit-onkeydown-js "window.event.ctrlKey && window.event.keyCode == 13 && document.getElementById('query-editor').form.submit();"
          on-cm-change-js "cm.save();"]
      (str
       "<!DOCTYPE html>"
       (html
        [:html
         (page-head "Message Board - Query Console")
         [:body
          [:header
           [:h2 [:a {:href ""} "Query Console"]]]
          [:div.query-editor
           [:form.submit-query {:action "/query" :method "GET" :title "Submit with Ctrl-Enter"}
            [:fieldset
             [:input {:type "hidden" :name "vt" :value (format-date vt)}]
             [:input {:type "hidden" :name "tt" :value (format-date tt)}]
             [(if invalid?
                :textarea#query-editor.invalid
                :textarea#query-editor)
              {:name "q" :required true :placeholder "Query"
               :rows (inc (count (str/split-lines (str q))))
               :oninput grow-textarea-oninput-js
               :onkeydown ctrl-enter-to-submit-onkeydown-js}
              (when (seq q)
                (str q))]
             [:script {:type "text/javascript"}
              (hiccup.util/raw-string
               (format "var opts = {lineNumbers: true, autofocus: true, mode: 'clojure', theme: 'eclipse', autoCloseBrackets: true, matchBrackets: true, keyMap: 'emacs'};
                        var cm = CodeMirror.fromTextArea(document.getElementById('query-editor'), opts);
                        cm.on('keydown', function() { %s });
                        cm.on('change', function() { %s });
                        cm.setSize('100%%', 'auto')"
                       ctrl-enter-to-submit-onkeydown-js on-cm-change-js))]
             (when invalid?
               [:div.invalid-query-message
                [:pre.edn (with-out-str
                            (pp/pprint (s/explain-data :crux.query/query q)))]])]
            [:input.primary {:type "submit" :value "Query"}]]
           (when-not query-invalid?
             [:div.query-result
              [:h4  (count result) " results:"
               [:small "(query time: " query-time "ms)"]]
              [:table
               [:thead
                (for [v (:find conformed-query)]
                  [:th (pr-str v)])]
               [:tbody
                (for [tuple result]
                  [:tr
                   (for [v tuple]
                     [:td
                      (with-date-links nil
                        (with-out-str
                          (pp/pprint v)))])])]]])]
          [:h5
           [:a {:href (str "/?tt=" (format-date tt) "&vt=" (format-date vt))} "Back to Message Board"]]
          (status-block crux)
          (footer)]])))))

(defn document-handler [ctx {:keys [crux]}]
  (fn [ctx]
    (let [content-hash (get-in ctx [:parameters :path :content-hash])]
      (when-let [document (and (c/valid-id? content-hash)
                               (api/document crux content-hash))]
        (str
         "<!DOCTYPE html>"
         (html
          [:html
           (page-head "Message Board - Document")
           [:body
            [:header
             [:h2 [:a {:href ""} "Document: " content-hash]]]
            [:pre.edn
             (with-date-links nil (with-out-str
                                    (pp/pprint document)))]
            [:h5
             [:a {:href "/"} "Back to Message Board"]]
            (status-block crux)
            (footer)]]))))))

(defn entity-handler [ctx {:keys [crux]}]
  (fn [ctx]
    (let [eid (get-in ctx [:parameters :path :eid])]
      (when-let [history (and (c/valid-id? eid)
                              (api/history crux eid))]
        (str
         "<!DOCTYPE html>"
         (html
          [:html
           (page-head "Message Board - Entity")
           [:body
            [:header
             [:h2 [:a {:href ""} "Entity: " eid]]]
            [:pre.edn (pp-entity-txs history)]
            [:h5
             [:a {:href "/"} "Back to Message Board"]]
            (status-block crux)
            (footer)]]))))))

(defn redirect-with-time [ctx valid-time transaction-time]
  (assoc (:response ctx)
         :status 302
         :headers {"location" (str "/?" "vt=" (format-date valid-time) "&tt=" (format-date transaction-time))}))

(defn post-comment-handler
  [ctx {:keys [crux]}]
  (let [{:keys [name message created _action]} (get-in ctx [:parameters :form])]
    (let [id (UUID/randomUUID)
          created (case (str/lower-case _action)
                    "comment" (Date.)
                    "bitemporal comment" (instant/read-instant-date created))
          {:keys [crux.tx/tx-time]}
          (api/submit-tx
           crux
           [[:crux.tx/put
             {:crux.db/id id
              :message-post/created created
              :message-post/name name
              :message-post/message message}
             created]])]
      (redirect-with-time ctx created tx-time))))

(defn edit-comment-handler
  [ctx {:keys [crux]}]
  (let [{:keys [name message created _action]} (get-in ctx [:parameters :form])
        id (UUID/fromString (get-in ctx [:parameters :path :id]))
        now (Date.)
        {:keys [crux.tx/tx-time]}
        (case (str/lower-case _action)
          "delete"
          (api/submit-tx
           crux
           [[:crux.tx/delete
             id
             now]])
          "delete history"
          (api/submit-tx
           crux
           [[:crux.tx/delete
             id
             (instant/read-instant-date created)
             now]])
          "evict"
          (api/submit-tx
           crux
           [[:crux.tx/evict
             id
             (instant/read-instant-date created)
             now]])
          "edit"
          (api/submit-tx
           crux
           [[:crux.tx/put
             {:crux.db/id id
              :message-post/created (instant/read-instant-date created)
              :message-post/edited now
              :message-post/name name
              :message-post/message message}
             now]]))]
    (redirect-with-time ctx now tx-time)))

(defn application-resource
  [node]
  ["/"
   [[""
     (resource
      {:methods
       {:get {:produces "text/html"
              :response #(index-handler % node)}}})]
    ["tx-log"
     (resource
      {:methods
       {:get {:produces "text/html"
              :response #(tx-log-handler % node)}}})]

    ["timeline"
     (resource
      {:methods
       {:get {:produces "text/html"
              :response #(timeline-graph-handler % node)}}})]

    [["document/" :content-hash]
     (resource
      {:methods
       {:get {:produces "text/html"
              :response #(document-handler % node)}}})]

    [["entity/" :eid]
     (resource
      {:methods
       {:get {:produces "text/html"
              :response #(entity-handler % node)}}})]

    ["query"
     (resource
      {:methods
       {:get {:produces "text/html"
              :response #(query-handler % node)}}})]

    ["comment"
     (resource
      {:methods
       {:post {:consumes "application/x-www-form-urlencoded"
               :parameters {:form {:created String
                                   :name String
                                   :message String
                                   :_action String}}
               :produces "text/html"
               :response #(post-comment-handler % node)}}})]

    [["comment/" :id]
     (resource
      {:methods
       {:post {:consumes "application/x-www-form-urlencoded"
               :parameters {:form {:created String
                                   :name String
                                   :message String
                                   :_action String}}
               :produces "text/html"
               :response #(edit-comment-handler % node)}}})]
    ["cljsjs"
     (yada.resources.classpath-resource/new-classpath-resource "cljsjs")]

    ["static"
     (yada.resources.classpath-resource/new-classpath-resource "static")]]])

(def index-dir "data/db-dir-1")
(def log-dir "data/eventlog-1")

(def crux-options
  {:crux.node/topology :crux.kafka/topology
   :crux.node/kv-store "crux.kv.rocksdb.RocksKv"
   :crux.kafka/bootstrap-servers "kafka-cluster-kafka-brokers.crux.svc.cluster.local:9092"
   :crux.standalone/event-log-dir log-dir
   :crux.kv/db-dir index-dir
   :server-port 8080

   :http-port 8081

   ::backup/backend ::backup/sh
   ::backup/checkpoint-directory "data/checkpoint"
   ::backup/sh-backup-script "bin/backup.sh"
   ::backup/sh-restore-script "bin/restore.sh"})

(defn run-node [{:keys [server-port http-port] :as options} with-node-fn]
  (with-open [crux-node (api/start-node options)
              api-server (srv/start-http-server
                           crux-node
                           {:server-port http-port
                            :cors-access-control
                            [:access-control-allow-origin [#".*"]
                             :access-control-allow-headers ["X-Requested-With"
                                                            "Content-Type"
                                                            "Cache-Control"
                                                            "Origin"
                                                            "Accept"
                                                            "Authorization"
                                                            "X-Custom-Header"]
                             :access-control-allow-methods [:get :put :post :delete]]})
              http-server
              (let [l (listener
                       (application-resource {:crux crux-node})
                       {:port server-port})]
                (log/info "started HTTP API on port:" http-port)
                (log/info "started webserver on port:" server-port)
                (reify Closeable
                  (close [_]
                    ((:close l)))))]
    (with-node-fn crux-node)))

(defn -main []
  (backup/check-and-restore crux-options)
  (try
    (run-node
      crux-options
      (fn [crux-node]
        (while (not (.isInterrupted (Thread/currentThread)))
          (Thread/sleep (* 1000 60 60 1)) ;; every hour
          (backup/backup-current-version crux-options crux-node))))
    (catch IndexVersionOutOfSyncException e
      (crux-io/delete-dir index-dir)
      (-main))
    (catch Exception e
      (log/error e "what happened" (ex-data e)))))

(defn start-from-repl []
  (def s
   (future
     (run-node
       crux-options
       (fn [_]
         (def crux)
         (Thread/sleep Long/MAX_VALUE))))))

(comment
  (start-from-repl)
  (future-cancel s))
