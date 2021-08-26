(ns xtdb.metrics.dropwizard
  (:import (com.codahale.metrics MetricRegistry
                                 Timer Timer$Context
                                 Gauge
                                 Metered Meter)
           clojure.lang.IFn))

;;;; Registry

(defn new-registry []
  (MetricRegistry.))

(defn- metric-name [path]
  (MetricRegistry/name "crux" ^"[Ljava.lang.String;" (into-array String path)))

;;;; Timers

(defn timer [^MetricRegistry reg title]
  (.timer reg (metric-name title)))

(defn start [^Timer t]
  (.time t))

(defn stop [^Timer$Context tc]
  (.stop tc))

;;;; Gauges

(defn gauge [^MetricRegistry reg title ^IFn f]
  (let [g (reify Gauge
            (getValue [this]
              (f)))
        s (metric-name title)]
    (.remove reg s)
    (.register reg s g)))

(defn value [^Gauge g]
  (.getValue g))

;;;; Meters

(defn meter [^MetricRegistry reg title]
  (.meter reg (metric-name title)))

(defn mark! [^Meter m ^long n]
  (doto m
    (.mark n)))

(defn meter-count [^Metered t]
  (.getCount t))
