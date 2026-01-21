(ns xtdb.bench.cloud.scripts.util
  "Common utilities for benchmark scripts."
  (:require [clojure.string :as str]))

(defn format-duration
  "Format a duration value to human-readable string.
   unit: :nanos, :micros, :millis, :seconds, :minutes, :hours
   value: numeric value in the specified unit"
  [unit value]
  (when value
    (let [nanos (case unit
                  :nanos value
                  :micros (* value 1e3)
                  :millis (* value 1e6)
                  :seconds (* value 1e9)
                  :minutes (* value 60e9)
                  :hours (* value 3600e9))]
      (cond
        (>= nanos 3600e9) (format "%.1fh" (/ nanos 3600e9))
        (>= nanos 60e9) (format "%.1fm" (/ nanos 60e9))
        (>= nanos 1e9) (format "%.2fs" (/ nanos 1e9))
        (>= nanos 1e6) (format "%.0fms" (/ nanos 1e6))
        (>= nanos 1e3) (format "%.0fus" (/ nanos 1e3))
        :else (format "%.0fns" (double nanos))))))

(defn title-case
  "Convert hyphenated string to title case."
  [s]
  (->> (str/split s #"-")
       (map str/capitalize)
       (str/join " ")))

(defn github-table
  "Generate a GitHub-flavored markdown table from columns and rows.

   columns: vector of {:key :column-key :header \"Column Header\" :format (optional fn)}
   rows: sequence of maps with keys matching column :key values"
  [columns rows]
  (let [headers (map :header columns)
        separator (map (fn [h] (apply str (repeat (max 3 (count h)) "-"))) headers)
        format-cell (fn [col value]
                      (let [formatter (or (:format col) str)]
                        (formatter value)))
        format-row (fn [row]
                     (str "| "
                          (->> columns
                               (map (fn [col]
                                      (format-cell col (get row (:key col)))))
                               (str/join " | "))
                          " |"))]
    (str "| " (str/join " | " headers) " |\n"
         "| " (str/join " | " separator) " |\n"
         (->> rows
              (map format-row)
              (str/join "\n")))))

(defn wrap-slack-code
  "Wrap strings in Slack code block."
  [& strings]
  (str "```\n" (str/join strings) "\n```"))

(defn totals->string
  "Format stage and benchmark totals as string.
   label: describes the type of stages summed (e.g. \"query\", \"ingest\", \"batch\")"
  ([stage-ms benchmark-ms] (totals->string stage-ms benchmark-ms "query"))
  ([stage-ms benchmark-ms label]
   (str (format "Total %s time: %s (%s)"
                label
                (or (format-duration :millis stage-ms) "N/A")
                (if stage-ms (java.time.Duration/ofMillis stage-ms) "N/A"))
        "\n"
        (format "Total benchmark time: %s (%s)"
                (or (format-duration :millis benchmark-ms) "N/A")
                (if benchmark-ms (java.time.Duration/ofMillis benchmark-ms) "N/A")))))
