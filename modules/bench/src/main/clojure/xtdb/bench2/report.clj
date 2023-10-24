(ns xtdb.bench2.report
  (:require [clojure.data.json :as json]
            [clojure.edn :as edn]
            [clojure.java.browse :as browse]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.cli :as cli]
            [juxt.clojars-mirrors.hiccup.v2v0v0-alpha2.hiccup2.core :as hiccup2])
  (:import (java.io File)))

;; use vega to plot metrics for now
;; works at repl, no servers needed
;; if this approach takes of the time series data wants to be graphed in something like a shared prometheus / grafana during run
(defn vega-plots [stages metric-data]
  (vec
   (for [[metric metric-data] (sort-by key (group-by :metric metric-data))]
     {:title metric
      :hconcat (vec (for [[[_statistic unit] metric-data] (sort-by key (group-by (juxt :statistic :unit) metric-data))
                          :let [data {:values (vec (for [{:keys [vs-label, series, samples]} metric-data
                                                         {:keys [time-ms, value]} samples
                                                         :when (Double/isFinite value)]
                                                     {:time (str time-ms)
                                                      :config vs-label
                                                      :series series
                                                      :value value}))
                                      :format {:parse {:time "utc:'%Q'"}}}
                                any-series (some (comp not-empty :series) metric-data)

                                series-dimension (and any-series (< 1 (count metric-data)))
                                vs-dimension (= 2 (bounded-count 2 (keep :vs-label metric-data)))

                                stack-series series-dimension
                                stack-vs (and (not stack-series) vs-dimension)
                                facet-vs (and vs-dimension (not stack-vs))

                                layer-instead-of-stack
                                (cond stack-series (str/ends-with? metric "percentile value")
                                      stack-vs true)

                                mark-type (if stack-vs "line" "area")

                                spec {:mark {:type mark-type, :line true, :tooltip true}
                                      :encoding {:x {:field "time"
                                                     :type "quantitative"
                                                     :timeUnit "utcdayofyearhoursminutessecondsmilliseconds"
                                                     :title "Time"}
                                                 :y (let [y {:field "value"
                                                             :type "quantitative"
                                                             :title (or unit "Value")}]
                                                      (if layer-instead-of-stack
                                                        (assoc y :stack false)
                                                        y))
                                                 :color
                                                 (cond
                                                   stack-series
                                                   {:field "series",
                                                    :legend {:labelLimit 280}
                                                    :type "nominal"}
                                                   stack-vs
                                                   {:field "config",
                                                    :legend {:labelLimit 280}
                                                    :type "nominal"})}}


                                stage-layer
                                {:data {:values (vec (for [{:keys [stage, start-ms, end-ms, vs-label]} stages]
                                                       {:stage stage
                                                        :start (str start-ms)
                                                        :end end-ms
                                                        :config vs-label}))
                                        :format {:parse {:start "utc:'%Q'"}}}
                                 :mark "point"
                                 :encoding {:x {:field "start"
                                                :type "quantitative"
                                                :timeUnit "utcdayofyearhoursminutessecondsmilliseconds"}
                                            :tooltip {:field "stage"}
                                            :color {:field "config",
                                                    :legend {:labelLimit 280}
                                                    :type "nominal"}}}]]
                      (if facet-vs
                        {:width 432
                         :data data
                         :facet {:column {:field "config"} :header {:title nil}}
                         ;; todo can I still layer the stage points?
                         :spec spec}
                        {:width 432
                         :layer [(assoc spec :data data) stage-layer]})))})))

(defn group-metrics [rs]
  (let [{:keys [metrics]} rs

        group-fn
        (fn [{:keys [meter]}]
          (condp #(str/starts-with? %2 %1) meter
            "bench." "001 - Benchmark"
            "node." "002 - XTDB Node"
            "jvm.gc" "003 - JVM Memory / GC"
            "jvm.memory" "003 - JVM Memory / GC"
            "jvm.buffer" "004 - JVM Buffer"
            "system." "005 - System / Process"
            "process." "005 - System / Process"
            "006 - Other"))

        metric-groups (group-by group-fn metrics)]

    metric-groups))

(def transaction-prefix "bench.transaction.")

(defn transaction-count [metrics]
  (->> metrics
       (filter
        (every-pred
         #(= "count" (:statistic %))
         #(= "count" (:unit %))
         #(str/starts-with? (:metric %) transaction-prefix)
         #(str/ends-with? (:metric %) " count")))
       (map (comp :value last :samples))
       (reduce +)))

(defn hiccup-report [title report]
  (let [id-from-thing
        (let [ctr (atom 0)]
          (memoize (fn [_] (str "id" (swap! ctr inc)))))
        systems (or (:systems report) [(:system report)])
        stage-metrics (group-by :stage (:metrics report))]
    (list
     [:html
      [:head
       [:title title]
       [:meta {:charset "utf-8"}]
       [:script {:src "https://cdn.jsdelivr.net/npm/vega@5.22.1"}]
       [:script {:src "https://cdn.jsdelivr.net/npm/vega-lite@5.6.0"}]
       [:script {:src "https://cdn.jsdelivr.net/npm/vega-embed@6.21.0"}]
       [:style {:media "screen"}
        ".vega-actions a {
          margin-right: 5px;
        }"]]
      [:body
       [:h1 title]

       (for [{:keys [label, system]} systems
             :let [{:keys [jre, max-heap, arch, os, cpu, memory, java-opts]} system]]
         [:div [:h2 label]
          [:table
           [:thead [:th "jre"] [:th "heap"] [:th "arch"] [:th "os"] [:th "cpu"] [:th "memory"]]
           [:tbody [:tr [:td jre] [:td max-heap] [:td arch] [:td os] [:td cpu] [:td memory]]]]
          [:pre java-opts]])

       [:div {:id "stage-summary"}]

       (for [stage (distinct (map :stage (:metrics report)))]
         [:div {:id (name stage)}])

       [:div
        (for [[group metric-data] (sort-by key (group-metrics report))]
          (list [:h2 group]
                (for [meter (sort (set (map :meter metric-data)))]
                  [:div {:id (id-from-thing meter)}])))]
       [:script
        (->> (concat
              [["stage-summary"
                {:data {:values (vec (for [{:keys [stage, end-ms, start-ms, vs-label]} (:stages report)]
                                       {:config vs-label
                                        :stage (name stage)
                                        :time (double (/ (- end-ms start-ms) 1e3))}))}
                 :mark "bar"
                 :encoding {:x {:field "stage", :type "nominal"}
                            :y {:field "time", :type "quantitative"}
                            :xOffset {:field "config", :type "nominal"}
                            :color {:field "config", :type "nominal"}}}]]

              (for [[stage metric-data] stage-metrics
                    :let [data {:values (vec (for [[vs-label metric-data] (group-by :vs-label metric-data)
                                                   [transaction metric-data] (group-by :meter metric-data)
                                                   :when (str/starts-with? transaction transaction-prefix)
                                                   :let [transaction (subs transaction (count transaction-prefix))
                                                         transactions (transaction-count metric-data)]
                                                   :when (pos? transactions)]
                                               {:config vs-label
                                                :transaction transaction
                                                :transactions transactions}))}
                          vega
                          {:title (name stage)
                           :data data
                           :hconcat [{:mark "bar"
                                      :encoding {:x {:field "config", :type "nominal", :sort "-y"}
                                                 :y {:field "transactions", :aggregate "sum", :type "quantitative"}}}
                                     {:mark "bar"
                                      :encoding {:x {:field "transaction", :type "nominal", :sort "-y"}
                                                 :y {:field "transactions", :type "quantitative"}
                                                 :xOffset {:field "config", :type "nominal"}
                                                 :color {:field "config", :type "nominal"}}}]}]
                    :when (->> data :values (map :transactions) (some pos?))]
                [(name stage) vega])

              (for [[meter metric-data] (group-by :meter (:metrics report))]
                [(id-from-thing meter) {:hconcat (vega-plots (:stages report) metric-data)}]))

             (map (fn [[id json]] (format "vegaEmbed('#%s', %s);" id (json/write-str json))))

             (str/join "\n")
             hiccup2/raw)]]])))

(defn show-html-report [rs]
  (let [f (File/createTempFile "xtdb-benchmark-report" ".html")]
    (spit f (hiccup2/html
             {}
             (hiccup-report (:title rs "Benchmark report") rs)))
    (browse/browse-url (io/as-url f))))

(defn- normalize-time [report]
  (let [{:keys [start-ms, stages, metrics]} report
        new-metrics (for [metric metrics
                          :let [{:keys [samples]} metric
                                new-samples (mapv #(update % :time-ms - start-ms) samples)]]
                      (assoc metric :samples new-samples))]
    (assoc report
           :stages (mapv #(-> % (update :start-ms - start-ms) (update :end-ms - start-ms)) stages)
           :metrics (vec new-metrics))))

(defn vs [label report & more]
  (let [pair-seq (cons [label report] (partition 2 more))
        ;; with order
        key-seq (map first pair-seq)
        ;; index without order
        report-map (apply hash-map label report more)
        report-map (update-vals report-map normalize-time)]
    {:title (str/join " vs " key-seq)
     :stages (vec (for [label key-seq
                        stage (:stages (report-map label))]
                    (assoc stage :vs-label label)))
     :systems (for [label key-seq] {:label label, :system (:system (report-map label))})
     :metrics (vec (for [[i label] (map-indexed vector key-seq)
                         :let [{:keys [metrics]} (report-map label)]
                         metric metrics]
                     (assoc metric :vs-label (str label))))}))

(defn stage-only [report stage]
  (update report :reports (partial filterv #(= stage (:stage %)))))

(def cli-options
  [[nil "--report [report-name,report-path]"
    :parse-fn (fn [s]
                (let [[name file] (-> s (subs 1 (dec (count s))) (str/split #","))]
                  [name file]))
    :assoc-fn (fn [m k v] (update m k (fnil conj []) v))]])

(defn -main [& args]
  (let [{:keys [options _arguments errors]} (cli/parse-opts args cli-options)]
    (if (seq errors)
      (binding [*out* *err*]
        (doseq [error errors]
          (println error))
        (System/exit 1))
      (let [reports (mapcat (fn [[name file-name]]
                              [name (-> file-name io/file slurp edn/read-string)]) (:report options))]

        (show-html-report (apply vs reports))
        (System/exit 0)))))
