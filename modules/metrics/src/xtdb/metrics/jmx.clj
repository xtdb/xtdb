(ns xtdb.metrics.jmx
  (:require [xtdb.metrics :as metrics]
            [xtdb.system :as sys])
  (:import com.codahale.metrics.jmx.JmxReporter
           com.codahale.metrics.MetricRegistry
           java.util.concurrent.TimeUnit))

(defn ->reporter {::sys/args {:domain {:doc "Add custom domain"
                                       :required? false
                                       :default "xtdb"
                                       :spec ::sys/string}
                              :rate-unit {:doc "Set rate unit"
                                          :required? false
                                          :default TimeUnit/SECONDS
                                          :spec ::sys/time-unit}
                              :duration-unit {:doc "Set duration unit"
                                              :required? false
                                              :default TimeUnit/MILLISECONDS
                                              :spec ::sys/time-unit}}
                  ::sys/deps {:registry ::metrics/registry
                              :metrics ::metrics/metrics}}
  ^com.codahale.metrics.jmx.JmxReporter
  [{:keys [^MetricRegistry registry domain rate-unit duration-unit]}]

  (-> (JmxReporter/forRegistry registry)
      (cond-> domain (.inDomain domain)
              rate-unit (.convertRatesTo rate-unit)
              duration-unit (.convertDurationsTo duration-unit))
      .build
      (doto (.start))))
