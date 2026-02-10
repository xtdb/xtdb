(ns xtdb.bench.cloud.scripts.charts
  "Chart generation for benchmark visualizations."
  (:require [babashka.process :as proc]
            [cheshire.core :as json]
            [xtdb.bench.cloud.scripts.azure :as azure]))

(defn plot-timeseries-vega
  "Plot a timeseries line chart and save to SVG using Vega-Lite.

  Requires vega-cli to be installed:
    npm install -g vega vega-lite vega-cli

  data: sequence of maps with :timestamp and :value keys
        e.g., [{:timestamp \"2025-01-15T10:30:00Z\" :value 1234.5}
               {:timestamp \"2025-01-16T11:00:00Z\" :value 1150.2}]

  opts: {:output-path \"chart.svg\"       ; where to save
         :title \"Benchmark Performance\"  ; chart title
         :x-label \"Date\"                 ; x-axis label (default: \"Time\")
         :y-label \"Duration (ms)\"        ; y-axis label (default: \"Value\")
         :width 800                       ; image width (default: 800)
         :height 600}                     ; image height (default: 600)"
  [data {:keys [output-path title x-label y-label width height]
         :or {x-label "Time"
              y-label "Value"
              width 800
              height 600}}]
  (let [;; Transform data for Vega-Lite (timestamp as string, value as number)
        ;; Ensure values are coerced to doubles
        vega-data (mapv (fn [{:keys [timestamp value]}]
                          {:timestamp timestamp :value (double value)})
                        data)

        ;; Calculate y-axis range (min - 2, max + 2 for padding)
        values (map :value vega-data)
        y-min (- (apply min values) 2.0)
        y-max (+ (apply max values) 2.0)

        ;; Create Vega-Lite spec
        vega-spec {:$schema "https://vega.github.io/schema/vega-lite/v5.json"
                   :title title
                   :width width
                   :height height
                   :data {:values vega-data}
                   :mark {:type "line" :point true}
                   :encoding {:x {:field "timestamp"
                                  :type "temporal"
                                  :title x-label
                                  :axis {:labelAngle -45}}
                              :y {:field "value"
                                  :type "quantitative"
                                  :title y-label
                                  :scale {:domain [y-min y-max]
                                          :reverse false}}}}

        ;; Write spec to temporary file
        temp-spec-file (str (java.io.File/createTempFile "vega-spec" ".json"))
        _ (spit temp-spec-file (json/generate-string vega-spec))]

    ;; Convert to SVG using vl2svg
    (proc/shell "vl2svg" temp-spec-file output-path)

    ;; Clean up temp file
    (.delete (java.io.File. temp-spec-file))))

(def ^:private benchmark-configs
  "Configuration for each benchmark type.
   :filter-param - parameter name in log.parameters to filter by
   :filter-value - default value to filter by
   :filter-is-string - true if filter-value is a string (for KQL quoting)
   :metric - log field to chart (default: time-taken-ms)
   :y-label - y-axis label (default: Duration (minutes))"
  {"tpch"       {:benchmark-name "TPC-H (OLAP)"
                 :title "TPC-H Benchmark Performance"
                 :filter-param "scale-factor"
                 :filter-value 1.0
                 :filter-is-string false}
   "yakbench"   {:benchmark-name "Yakbench"
                 :title "Yakbench Benchmark Performance"
                 :filter-param "scale-factor"
                 :filter-value 1.0
                 :filter-is-string false}
   "auctionmark" {:benchmark-name "Auction Mark OLTP"
                  :title "AuctionMark Throughput"
                  :filter-param "duration"
                  :filter-value "PT30M"
                  :filter-is-string true
                  :metric "throughput"
                  :y-label "Throughput (tx/s)"}
   "readings"   {:benchmark-name "Readings benchmarks"
                 :title "Readings Benchmark Performance"
                 :filter-param "devices"
                 :filter-value 10000
                 :filter-is-string false}
   "clickbench" {:benchmark-name "Clickbench Hits"
                 :title "Clickbench Benchmark Performance"
                 :filter-param nil
                 :filter-value nil
                 :filter-is-string false}
   "fusion"     {:benchmark-name "Fusion benchmark"
                 :title "Fusion Throughput"
                 :filter-param "devices"
                 :filter-value 10000
                 :filter-is-string false
                 :metric "throughput"
                 :y-label "Throughput (tx/s)"}
   "tsbs-iot"   {:benchmark-name "TSBS IoT"
                 :title "TSBS IoT Benchmark Performance"
                 :filter-param "devices"
                 :filter-value 2000
                 :filter-is-string false}
   "ingest-tx-overhead" {:benchmark-name "Ingest batch vs individual"
                         :title "Ingest TX Overhead Benchmark Performance"
                         :filter-param "doc-count"
                         :filter-value 100000
                         :filter-is-string false}
   "patch"    {:benchmark-name "PATCH Performance Benchmark"
               :title "Patch Benchmark Performance"
               :filter-param "doc-count"
               :filter-value 500000
               :filter-is-string false}
   "products" {:benchmark-name "Products"
               :title "Products Benchmark Performance"
               :filter-param nil
               :filter-value nil
               :filter-is-string false}
   "ts-devices" {:benchmark-name "TS Devices Ingest"
                 :title "TS Devices Benchmark Performance"
                 :filter-param "size"
                 :filter-value "small"
                 :filter-is-string true}})

(defn plot-benchmark-timeseries
  "Plot a benchmark timeseries chart from Azure Log Analytics.

  benchmark-type: benchmark type (e.g., \"tpch\", \"yakbench\", \"auctionmark\", \"readings\")
  opts: {:filter-value <val>   ; override the default filter value for this benchmark
         :repo \"owner/repo\"   ; override github repo (default: xtdb/xtdb)
         :branch \"branch\"}    ; override git branch (default: main)

  Fetches benchmark data and plots it to an SVG file.
  Uses default parameters suitable for the specified benchmark type."
  ([benchmark-type] (plot-benchmark-timeseries benchmark-type {}))
  ([benchmark-type opts]
   (let [config (get benchmark-configs benchmark-type)
         _ (when-not config
             (throw (ex-info (format "Unsupported benchmark type for timeseries plotting: %s" benchmark-type)
                             {:benchmark-type benchmark-type
                              :supported (keys benchmark-configs)})))
         {:keys [benchmark-name title filter-param filter-value filter-is-string
                 metric y-label]} config
         metric (or metric "time-taken-ms")
         y-label (or y-label "Duration (minutes)")
         duration-metric? (= metric "time-taken-ms")
         ;; Allow override of filter-value via opts
         actual-filter-value (get opts :filter-value filter-value)
         fetch-opts (cond-> {:benchmark benchmark-name
                             :metric metric}
                      filter-param (assoc :filter-param filter-param
                                          :filter-value actual-filter-value
                                          :filter-is-string filter-is-string)
                      (:repo opts) (assoc :repo (:repo opts))
                      (:branch opts) (assoc :branch (:branch opts)))
         raw-data (azure/fetch-azure-benchmark-timeseries fetch-opts)
         data (mapv (fn [{:keys [timestamp value]}]
                      (let [num-value (if (string? value)
                                        (Double/parseDouble value)
                                        (double value))]
                        {:timestamp timestamp
                         :value (if duration-metric?
                                  (/ num-value 60000.0)
                                  num-value)}))
                    raw-data)
         output-path (str benchmark-type "-benchmark-timeseries.svg")
         chart-title (if (and filter-param actual-filter-value)
                       (str title " (" filter-param "=" actual-filter-value ")")
                       title)]
     (plot-timeseries-vega data {:output-path output-path
                                 :title chart-title
                                 :x-label "Time"
                                 :y-label y-label})
     (println "Generated chart:" output-path)
     output-path)))
