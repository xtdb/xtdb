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
  {"tpch"       {:benchmark-name "TPC-H (OLAP)"
                 :title "TPC-H Benchmark Performance"
                 :default-scale-factor 1.0}
   "yakbench"   {:benchmark-name "Yakbench"
                 :title "Yakbench Benchmark Performance"
                 :default-scale-factor 1.0}
   "auctionmark" {:benchmark-name "Auction Mark OLTP"
                  :title "AuctionMark Benchmark Performance"
                  :default-scale-factor 0.1}
   "readings"   {:benchmark-name "Readings benchmarks"
                 :title "Readings Benchmark Performance"
                 :default-scale-factor nil} ;; readings doesn't use scale-factor
   "clickbench" {:benchmark-name "Clickbench Hits"
                 :title "Clickbench Benchmark Performance"
                 :default-scale-factor nil} ;; clickbench uses size, not scale-factor
   "tsbs-iot"   {:benchmark-name "TSBS IoT"
                 :title "TSBS IoT Benchmark Performance"
                 :default-scale-factor nil} ;; tsbs-iot uses devices, not scale-factor
   "ingest-tx-overhead" {:benchmark-name "Ingest batch vs individual"
                         :title "Ingest TX Overhead Benchmark Performance"
                         :default-scale-factor nil} ;; ingest-tx-overhead uses doc-count, not scale-factor
   "patch"    {:benchmark-name "PATCH Performance Benchmark"
               :title "Patch Benchmark Performance"
               :default-scale-factor nil} ;; patch uses doc-count, not scale-factor
   "products" {:benchmark-name "Products"
               :title "Products Benchmark Performance"
               :default-scale-factor nil} ;; products uses limit, not scale-factor
   "ts-devices" {:benchmark-name "TS Devices Ingest"
                 :title "TS Devices Benchmark Performance"
                 :default-scale-factor nil}}) ;; ts-devices uses size, not scale-factor

(defn plot-benchmark-timeseries
  "Plot a benchmark timeseries chart from Azure Log Analytics.

  benchmark-type: benchmark type (e.g., \"tpch\", \"yakbench\", \"auctionmark\", \"readings\")
  opts: {:scale-factor 1.0}  ; scale factor to filter by (uses default if not provided)

  Fetches benchmark data and plots it to an SVG file.
  Uses default parameters suitable for the specified benchmark type."
  ([benchmark-type] (plot-benchmark-timeseries benchmark-type {}))
  ([benchmark-type {:keys [scale-factor]}]
   (let [config (get benchmark-configs benchmark-type)
         _ (when-not config
             (throw (ex-info (format "Unsupported benchmark type for timeseries plotting: %s" benchmark-type)
                             {:benchmark-type benchmark-type
                              :supported (keys benchmark-configs)})))
         {:keys [benchmark-name title default-scale-factor]} config
         sf (or scale-factor default-scale-factor)
         fetch-opts (cond-> {:benchmark benchmark-name}
                      sf (assoc :scale-factor sf))
         data-ms (azure/fetch-azure-benchmark-timeseries fetch-opts)
         ;; Convert milliseconds to minutes
         data (mapv (fn [{:keys [timestamp value]}]
                      (let [num-value (if (string? value)
                                        (Double/parseDouble value)
                                        (double value))]
                        {:timestamp timestamp
                         :value (/ num-value 60000.0)}))
                    data-ms)
         output-path (str benchmark-type "-benchmark-timeseries.svg")
         chart-title (if sf
                       (str title " (SF " sf ")")
                       title)]
     (plot-timeseries-vega data {:output-path output-path
                                 :title chart-title
                                 :x-label "Time"
                                 :y-label "Duration (minutes)"})
     (println "Generated chart:" output-path)
     output-path)))
