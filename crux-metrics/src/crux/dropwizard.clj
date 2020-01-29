(ns crux.dropwizard
  (:import [com.codahale.metrics MetricRegistry Metric Timer Timer$Context Gauge Meter]
           clojure.lang.IFn))

;; # Registry

(defn ^com.codahale.metrics.MetricRegistry new-registry
  []
  (MetricRegistry.))

(defn ^String metric-name
  [title]
  (if (string? title)
    (MetricRegistry/name "default"
                         ^"[Ljava.lang.String;" (into-array String ["default" ^String title]))
    (MetricRegistry/name
     ^String (first title)
     ^"[Ljava.lang.String;" (into-array String
                                        (if (= 3 (count title))
                                          [(second title) (last title)]
                                          (rest title))))))

(defn add-metric
  "Add a metric with the given title."
  [^MetricRegistry reg title ^Metric metric]
  (.register reg (metric-name title) metric))

(defn remove-metric
  "Remove the metric with the given title."
  [^MetricRegistry reg title]
  (.remove reg (metric-name title)))

;; # Timers

(defn ^com.codahale.metrics.Timer timer
  [^MetricRegistry reg title]
  (.timer reg (metric-name title)))

(defn start ^com.codahale.metrics.Timer$Context
  [^Timer t]
  (.time t))

(defn stop
  [^Timer$Context tc]
  (.stop tc))

(defn number-recorded ^long
  [^Timer t]
  (.getCount t))

;; # Gauges

(defn gauge-fn
  [^MetricRegistry reg title ^IFn f]
  (let [g (reify Gauge
            (getValue [this]
              (f)))
        s (metric-name title)]
    (.remove reg s)
    (.register reg s g)))

(defn value
  [^Gauge g]
  (.getValue g))

;; # Meters

(defn meter
  [^MetricRegistry reg title]
  (.meter reg (metric-name title)))


(defn mark!
  [^Meter m n]
  (.mark m (long n))
  m)

(defn mark-count
  [^Meter m]
  (.getCount m))
