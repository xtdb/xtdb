(ns xtdb.logging
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.tools.logging.impl :as log-impl])
  (:import [org.apache.logging.log4j Level LogManager]
           [org.apache.logging.log4j.core.config Configurator]))

(alter-var-root #'log/*logger-factory* (constantly (log-impl/log4j2-factory)))

(defn set-log-level! [ns level]
  (Configurator/setLevel (name ns) (when level
                                     (Level/valueOf (str/upper-case (name level))))))

(defn get-log-level! [ns]
  (some-> (.getLevel (LogManager/getLogger (name ns)))
          (str)
          (str/lower-case)
          (keyword)))

(defn with-log-levels* [ns-level-pairs f]
  (let [nses (mapv first ns-level-pairs)
        orig-levels (mapv get-log-level! nses)]
    (try
      (doseq [[ns l] ns-level-pairs] (set-log-level! ns l))
      (f)
      (finally
        (doseq [[ns ol] (mapv vector nses orig-levels)]
          (set-log-level! ns ol))))))

(defmacro with-log-levels [ns-level-pairs & body]
  `(with-log-levels* ~ns-level-pairs (fn [] ~@body)))

(defmacro with-log-level [ns level & body]
  `(with-log-levels {~ns ~level} ~@body))

(defn- env-var->log-ns [env-var]
  (when-let [[_ extras] (re-matches #"xtdb_logging_level(?:_([a-z\-]+(?:_[a-z\-]+)*))?" (str/lower-case env-var))]
    (->> (into ["xtdb"] (some-> extras (str/split #"_")))
         (str/join ".")
         symbol)))

(defn set-from-env! [env]
  (doseq [[k v] env]
    (when-let [log-ns (env-var->log-ns k)]
      (set-log-level! log-ns v))))

