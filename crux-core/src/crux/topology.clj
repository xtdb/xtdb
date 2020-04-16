(ns ^:no-doc crux.topology
  (:require [clojure.spec.alpha :as s]
            [com.stuartsierra.dependency :as dep]
            [crux.io :as cio])
  (:import (java.io Closeable)
           (crux.api ICruxAPI)))

(s/def ::resolvable-id
  (fn [id]
    (and (or (string? id) (keyword? id) (symbol? id))
         (namespace (symbol id)))))

(defn- resolve-id [id]
  (s/assert ::resolvable-id id)
  (-> (or (-> id symbol requiring-resolve)
          (throw (IllegalArgumentException. (format "Can't resolve symbol: '%s'" id))))
      var-get))

(s/def ::start-fn ifn?)
(s/def ::deps (s/coll-of keyword?))

(s/def ::args
  (s/map-of keyword?
            (s/keys :req [:crux.config/type]
                    :req-un [:crux.config/doc]
                    :opt-un [:crux.config/default
                             :crux.config/required?])))

(s/def ::component
  (s/and (s/or :component-id ::resolvable-id, :component map?)
         (s/conformer (fn [[c-or-id s]]
                        (cond-> s (= :component-id c-or-id) resolve-id)))
         (s/keys :req-un [::start-fn]
                 :opt-un [::deps ::args])))

(s/def ::module
  (s/and (s/or :module-id ::resolvable-id, :module map?)
         (s/conformer (fn [[m-or-id s]]
                        (cond-> s (= :module-id m-or-id) resolve-id)))))

(s/def ::resolved-topology (s/map-of keyword? ::component))

(defn options->topology [{:keys [crux.node/topology] :as options}]
  (when-not topology
    (throw (IllegalArgumentException. "Please specify :crux.node/topology")))

  (let [topology (-> topology
                     (cond-> (not (vector? topology)) vector)
                     (->> (map #(s/conform ::module %))
                          (apply merge))
                     (as-> topology (merge topology (select-keys options (keys topology)))))
        resolved-topology (s/conform ::resolved-topology topology)]
    (when (s/invalid? resolved-topology)
      (s/explain ::resolved-topology topology)
      (throw (IllegalArgumentException. "invalid topology")))
    resolved-topology))

(defn- start-order [system]
  (let [g (reduce-kv (fn [g k c]
                       (let [{:keys [deps before]} (s/conform ::component c)]
                         (-> g
                             (as-> g (reduce (fn [g d] (dep/depend g k d)) g deps))
                             (as-> g (reduce (fn [g b] (dep/depend g b k)) g before)))))
                     (dep/graph)
                     system)
        dep-order (dep/topo-sort g)
        dep-order (->> (keys system)
                       (remove #(contains? (set dep-order) %))
                       (into dep-order))]
    dep-order))

(defn parse-opts [args options]
  (into {}
        (for [[k {:keys [crux.config/type default required?]}] args]
          (let [v (get options k default)
                _ (when (and required? (nil? v))
                    (throw (IllegalArgumentException. (format "Arg %s required" k))))
                v (some->> v (s/conform type))]

            (if (s/invalid? v)
              (throw (IllegalArgumentException. (format "Arg %s invalid: %s" k (s/explain-str type v))))
              [k v])))))

(defn start-component [c started options]
  (s/assert ::component c)
  (let [{:keys [start-fn deps spec args]} (s/conform ::component c)
        deps (select-keys started deps)
        options (merge options (parse-opts args options))]
    (start-fn deps options)))

(defn- close-topology [started-order]
  (->> (reverse started-order)
       (filter #(instance? Closeable %))
       (remove #(instance? ICruxAPI %)) ; not pretty, but prevents infinite loop
       (run! cio/try-close)))

(defn start-topology [options]
  (let [options (into {} options)
        topology (options->topology options)]
    (s/assert ::resolved-topology topology)
    (let [started-order (atom [])
          started (atom {})
          started-modules (try
                            (into {}
                                  (for [k (start-order topology)]
                                    (let [c (or (get topology k)
                                                (throw (IllegalArgumentException. (str "Could not find component " k))))
                                          c (start-component c @started options)]
                                      (swap! started-order conj c)
                                      (swap! started assoc k c)
                                      [k c])))
                            (catch Throwable t
                              (close-topology @started-order)
                              (throw t)))]
      [started-modules (fn []
                         (close-topology @started-order))])))
